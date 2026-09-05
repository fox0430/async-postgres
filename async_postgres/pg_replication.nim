## PostgreSQL Logical Replication support.
##
## Provides types and procedures for consuming a logical replication stream
## via the PostgreSQL streaming replication protocol. The streaming API is
## plugin-agnostic (raw WAL bytes are delivered to a callback). A built-in
## decoder for the ``pgoutput`` logical decoding plugin is included.
##
## Quick start
## ===========
## .. code-block:: nim
##   let conn = await connectReplication("postgresql://user:pass@host/db")
##   defer: await conn.close()
##   let slot = await conn.createReplicationSlot("my_slot", "pgoutput", temporary = true)
##   await conn.startReplication("my_slot", slot.consistentPoint,
##       options = {"proto_version": "'1'", "publication_names": "'my_pub'"},
##       callback = myCallback)

import std/[strutils, tables, times, options]

import async_backend, pg_protocol, pg_connection, pg_types
import pg_connection/[types, dsn, buffer_io, simple_query, lifecycle]
import pg_types/encoding

type
  Lsn* = distinct uint64
    ## LSN (Log Sequence Number)
    ## PostgreSQL Log Sequence Number. Displayed as ``"X/Y"`` where X and Y
    ## are hex-encoded upper and lower 32-bit halves.

  ReplicationMessageKind* = enum
    ## Replication message types (decoded from CopyData during streaming)
    rmkXLogData
    rmkPrimaryKeepalive

  XLogData* = object ## WAL data payload from the server.
    startLsn*: Lsn ## Start LSN of the WAL data in this message
    walEnd*: Lsn
      ## Current end of WAL on the server at the time this message was sent.
      ## This is *not* the end of the WAL data contained in this message; it
      ## reflects how far WAL has advanced on the server and is informational.
      ## To acknowledge what was actually received, use ``receivedEndLsn``
      ## (``startLsn + data.len``), never ``walEnd`` — ``walEnd`` may be ahead
      ## of what this message contains.
    sendTime*: int64 ## Server send time (microseconds since PG epoch)
    data*: seq[byte] ## Raw WAL data (plugin-dependent format)

  PrimaryKeepalive* = object ## Keepalive message from the server.
    walEnd*: Lsn ## Current end of WAL on the server
    sendTime*: int64 ## Server send time (microseconds since PG epoch)
    replyRequested*: bool ## Whether the server wants an immediate status reply

  ReplicationMessage* = object
    ## A single message received during replication streaming.
    case kind*: ReplicationMessageKind
    of rmkXLogData:
      xlogData*: XLogData
    of rmkPrimaryKeepalive:
      keepalive*: PrimaryKeepalive

  ReplicationSlotInfo* = object ## Information about a replication slot.
    slotName*: string
    consistentPoint*: Lsn ## confirmed_flush_lsn (logical) or restart_lsn (physical)
    snapshotName*: string ## Snapshot name (only available at CREATE time)
    outputPlugin*: string

  SystemInfo* = object ## Result of IDENTIFY_SYSTEM command.
    systemId*: string
    timeline*: int32
    xLogPos*: Lsn
    dbName*: string

  ReplicationMode* = enum
    ## Replication mode selected at connection time.
    ## ``rmDatabase`` sends ``replication=database`` (logical replication +
    ## ability to run SQL on the chosen database). ``rmPhysical`` sends
    ## ``replication=true`` (physical replication; no SQL on user databases).
    rmDatabase
    rmPhysical

  TimelineHistory* = object ## Result of TIMELINE_HISTORY command.
    filename*: string ## Timeline history file name (e.g. "00000002.history").
    content*: seq[byte] ## Raw history file content.

  # pgoutput decoder types
  PgOutputMessageKind* = enum
    ## Message types within the pgoutput logical decoding plugin.
    pomkBegin
    pomkCommit
    pomkOrigin
    pomkRelation
    pomkType
    pomkInsert
    pomkUpdate
    pomkDelete
    pomkTruncate
    pomkMessage

  RelationColumn* = object ## A single column in a relation definition.
    flags*: byte ## Bit 0: part of replica identity key
    name*: string
    typeOid*: int32
    typeMod*: int32

  RelationInfo* = object
    ## Relation (table) metadata sent by pgoutput before DML events.
    relationId*: int32
    namespace*: string ## Schema name
    name*: string ## Table name
    replicaIdentity*: char ## 'd' (default), 'n' (nothing), 'f' (full), 'i' (index)
    columns*: seq[RelationColumn]

  TupleDataKind* = enum
    ## Kind of a single field value in a pgoutput tuple.
    tdkNull = 'n' ## NULL value
    tdkText = 't' ## Text-formatted value
    tdkBinary = 'b' ## Binary-formatted value (protocol_version >= 2)
    tdkUnchanged = 'u' ## TOAST value unchanged

  TupleField* = object ## A single field value in a pgoutput tuple.
    kind*: TupleDataKind
    data*: seq[byte] ## Empty for null/unchanged

  BeginMessage* = object ## Transaction begin.
    finalLsn*: Lsn ## LSN of the commit record
    commitTime*: int64 ## Commit timestamp (microseconds since PG epoch)
    xid*: int32 ## Transaction ID

  CommitMessage* = object ## Transaction commit.
    flags*: byte
    commitLsn*: Lsn
    endLsn*: Lsn
    commitTime*: int64

  OriginMessage* = object ## Replication origin.
    originLsn*: Lsn
    originName*: string

  TypeMessage* = object ## Custom type definition.
    typeId*: int32
    namespace*: string
    name*: string

  InsertMessage* = object ## Row insertion.
    relationId*: int32
    newTuple*: seq[TupleField]

  UpdateMessage* = object ## Row update.
    relationId*: int32
    keyKind*: char
      ## 'K' if oldTuple holds only the replica identity key,
      ## 'O' if it holds the full old row (REPLICA IDENTITY FULL),
      ## '\0' when no old tuple is present.
    oldTuple*: seq[TupleField]
    newTuple*: seq[TupleField]

  DeleteMessage* = object ## Row deletion.
    relationId*: int32
    keyKind*: char
      ## 'K' if oldTuple holds only the replica identity key,
      ## 'O' if it holds the full old row (REPLICA IDENTITY FULL).
    oldTuple*: seq[TupleField]

  TruncateMessage* = object ## Table truncation.
    options*: byte ## Bit 0: CASCADE, bit 1: RESTART IDENTITY
    relationIds*: seq[int32]

  LogicalMessage* = object
    ## Generic logical decoding message (via pg_logical_emit_message).
    flags*: byte ## Bit 0: transactional
    lsn*: Lsn
    prefix*: string
    content*: seq[byte]

  PgOutputMessage* = object ## A decoded pgoutput plugin message.
    case kind*: PgOutputMessageKind
    of pomkBegin:
      begin*: BeginMessage
    of pomkCommit:
      commit*: CommitMessage
    of pomkOrigin:
      origin*: OriginMessage
    of pomkRelation:
      relation*: RelationInfo
    of pomkType:
      typeMsg*: TypeMessage
    of pomkInsert:
      insert*: InsertMessage
    of pomkUpdate:
      update*: UpdateMessage
    of pomkDelete:
      delete*: DeleteMessage
    of pomkTruncate:
      truncate*: TruncateMessage
    of pomkMessage:
      message*: LogicalMessage

  RelationCache* = Table[int32, RelationInfo]
    ## Cache of relation metadata received during replication.
    ## The server sends a Relation message before the first DML for
    ## each table in a transaction; clients must cache them.

const
  InvalidLsn* = Lsn(0) ## Sentinel value representing an invalid or unset LSN.

  MaxRelationColumns = 1600
    ## PostgreSQL's max columns per table (``MaxHeapAttributeNumber``).
    ## pgoutput's column-count wire field can never exceed this in practice.

proc `==`*(a, b: Lsn): bool {.borrow.}
proc `<`*(a, b: Lsn): bool {.borrow.}
proc `<=`*(a, b: Lsn): bool {.borrow.}

proc hasOldTuple*(msg: UpdateMessage): bool {.inline.} =
  ## True if the update carries an old tuple (replica identity key or full row).
  msg.keyKind != '\0'

proc toString*(field: TupleField): string =
  ## Convert a TupleField's data to a string by copying the bytes.
  result = readString(field.data, 0, field.data.len)

template toUInt64*(lsn: Lsn): uint64 =
  ## Get the raw uint64 value of an LSN.
  uint64(lsn)

template toInt64*(lsn: Lsn): int64 =
  ## Get the LSN as int64 (for wire protocol encoding).
  cast[int64](uint64(lsn))

proc stripLeadingZeros(s: string): string =
  var i = 0
  while i < s.len - 1 and s[i] == '0':
    inc i
  s[i ..< s.len]

proc `$`*(lsn: Lsn): string =
  ## Format an LSN as ``"X/Y"`` hex string.
  let v = lsn.toUInt64
  let hi = v shr 32
  let lo = v and 0xFFFF_FFFF'u64
  stripLeadingZeros(toHex(uint32(hi))) & "/" & stripLeadingZeros(toHex(uint32(lo)))

proc parseLsn*(s: string): Lsn =
  ## Parse an LSN from ``"X/Y"`` hex string. Converts a malformed value (wrong
  ## shape, non-hex halves, or a half wider than 32 bits) into `PgTypeError`
  ## so callers stay under the ``except PgError`` contract, mirroring
  ## `parseTimelineId`.
  let parts = s.split('/')
  if parts.len != 2:
    raise newException(PgTypeError, "Invalid LSN format: " & s)
  # fromHex[uint64] returns 0 for an empty string instead of raising, so an
  # empty half would silently produce a zero LSN — reject explicitly.
  if parts[0].len == 0 or parts[1].len == 0:
    raise newException(PgTypeError, "Invalid LSN format: " & s)
  # fromHex[uint64] wraps silently past 16 significant hex digits instead of
  # raising; compare significant digits, not raw length, so a zero-padded but
  # in-range half isn't rejected.
  if stripLeadingZeros(parts[0]).len > 16 or stripLeadingZeros(parts[1]).len > 16:
    raise newException(PgTypeError, "Invalid LSN format: " & s)
  pgTypeErrorOnValueError("Invalid LSN format: " & s):
    let hi = fromHex[uint64](parts[0])
    let lo = fromHex[uint64](parts[1])
    # A half > 32 bits would have its excess bits silently dropped by `hi shl 32` below.
    if hi > 0xFFFF_FFFF'u64 or lo > 0xFFFF_FFFF'u64:
      raise newException(PgTypeError, "Invalid LSN format: " & s)
    Lsn((hi shl 32) or lo)

# PostgreSQL timestamp helpers

proc currentPgTimestamp*(): int64 =
  ## Current time as microseconds since the PostgreSQL epoch (2000-01-01 UTC).
  pgTimestampMicros(getTime())

# pgoutput decoder

# Bounds-checked readers for the pgoutput decoder.
#
# The raw ``decodeInt16``/``decodeInt32``/``decodeInt64`` helpers index the
# buffer directly and rely on Nim's array bounds checks. Those checks are
# compiled out under ``-d:danger`` (and raise the uncatchable ``IndexDefect``
# otherwise), so feeding a truncated or malicious WAL stream through the
# decoder could read past the end of the buffer. These wrappers validate the
# available length first and raise ``PgProtocolError`` (a ``CatchableError`` and
# ``PgError`` subtype) on any shortfall, matching how the rest of the wire
# parsing reports protocol violations.

proc ensureAvail(buf: openArray[byte], pos, n: int) {.inline.} =
  ## Raise ``PgProtocolError`` unless ``n`` bytes are readable at ``pos``.
  ## ``n > buf.len - pos`` is written so it cannot overflow and so a ``pos``
  ## past the end (negative ``buf.len - pos``) is rejected for any ``n >= 0``.
  if pos < 0 or n < 0 or n > buf.len - pos:
    raise newException(
      PgProtocolError,
      "pgoutput: truncated message (need " & $n & " byte(s) at offset " & $pos &
        ", buffer holds " & $buf.len & ")",
    )

proc readByteAt(buf: openArray[byte], pos: int): byte {.inline.} =
  ensureAvail(buf, pos, 1)
  buf[pos]

proc readInt16At(buf: openArray[byte], pos: int): int16 {.inline.} =
  ensureAvail(buf, pos, 2)
  decodeInt16(buf, pos)

proc readInt32At(buf: openArray[byte], pos: int): int32 {.inline.} =
  ensureAvail(buf, pos, 4)
  decodeInt32(buf, pos)

proc readInt64At(buf: openArray[byte], pos: int): int64 {.inline.} =
  ensureAvail(buf, pos, 8)
  decodeInt64(buf, pos)

proc readBytesAt(buf: openArray[byte], pos, n: int): seq[byte] {.inline.} =
  ## ``n`` is attacker-controlled (a length prefix from the stream); validate it
  ## against the buffer before the bulk copy in ``readBytes``.
  ensureAvail(buf, pos, n)
  readBytes(buf, pos, n)

proc readColumnCountAt(
    buf: openArray[byte], pos: int, context: string
): int16 {.inline.} =
  ## Read a pgoutput column-count field, bounded by ``MaxRelationColumns``.
  result = readInt16At(buf, pos)
  if result < 0 or result.int > MaxRelationColumns:
    raise newException(PgProtocolError, context & ": invalid column count " & $result)

proc decodeCStringAt(buf: openArray[byte], offset: int): (string, int) =
  ## Decode a null-terminated string at offset. Returns (string, next offset).
  let (s, consumed) = decodeCString(buf, offset)
  (s, offset + consumed)

proc decodeTuple(buf: openArray[byte], offset: int): (seq[TupleField], int) =
  ## Decode a pgoutput TupleData structure.
  var pos = offset
  let numCols = readColumnCountAt(buf, pos, "pgoutput tuple")
  pos += 2
  var fields = newSeq[TupleField](numCols)
  for i in 0 ..< numCols:
    let kind = char(readByteAt(buf, pos))
    inc pos
    case kind
    of 'n':
      fields[i] = TupleField(kind: tdkNull)
    of 'u':
      fields[i] = TupleField(kind: tdkUnchanged)
    of 't', 'b':
      let dataLen = readInt32At(buf, pos)
      pos += 4
      let data = readBytesAt(buf, pos, int(dataLen))
      pos += int(dataLen)
      fields[i] = TupleField(kind: if kind == 't': tdkText else: tdkBinary, data: data)
    else:
      raise newException(PgProtocolError, "Unknown tuple field kind: " & kind)
  (fields, pos)

proc parsePgOutputMessage*(data: openArray[byte]): PgOutputMessage =
  ## Decode a pgoutput logical decoding message from raw WAL bytes.
  if data.len == 0:
    raise newException(PgProtocolError, "Empty pgoutput message")
  let msgType = char(data[0])
  case msgType
  of 'B': # Begin
    var msg = BeginMessage()
    msg.finalLsn = Lsn(cast[uint64](readInt64At(data, 1)))
    msg.commitTime = readInt64At(data, 9)
    msg.xid = readInt32At(data, 17)
    PgOutputMessage(kind: pomkBegin, begin: msg)
  of 'C': # Commit
    var msg = CommitMessage()
    msg.flags = readByteAt(data, 1)
    msg.commitLsn = Lsn(cast[uint64](readInt64At(data, 2)))
    msg.endLsn = Lsn(cast[uint64](readInt64At(data, 10)))
    msg.commitTime = readInt64At(data, 18)
    PgOutputMessage(kind: pomkCommit, commit: msg)
  of 'O': # Origin
    var msg = OriginMessage()
    msg.originLsn = Lsn(cast[uint64](readInt64At(data, 1)))
    let (name, _) = decodeCStringAt(data, 9)
    msg.originName = name
    PgOutputMessage(kind: pomkOrigin, origin: msg)
  of 'R': # Relation
    var msg = RelationInfo()
    msg.relationId = readInt32At(data, 1)
    var pos = 5
    let (ns, pos2) = decodeCStringAt(data, pos)
    msg.namespace = ns
    pos = pos2
    let (name, pos3) = decodeCStringAt(data, pos)
    msg.name = name
    pos = pos3
    msg.replicaIdentity = char(readByteAt(data, pos))
    inc pos
    let numCols = readColumnCountAt(data, pos, "pgoutput Relation")
    pos += 2
    msg.columns = newSeq[RelationColumn](numCols)
    for i in 0 ..< numCols:
      var col = RelationColumn()
      col.flags = readByteAt(data, pos)
      inc pos
      let (colName, nextPos) = decodeCStringAt(data, pos)
      col.name = colName
      pos = nextPos
      col.typeOid = readInt32At(data, pos)
      pos += 4
      col.typeMod = readInt32At(data, pos)
      pos += 4
      msg.columns[i] = col
    PgOutputMessage(kind: pomkRelation, relation: msg)
  of 'Y': # Type
    var msg = TypeMessage()
    msg.typeId = readInt32At(data, 1)
    var pos = 5
    let (ns, pos2) = decodeCStringAt(data, pos)
    msg.namespace = ns
    pos = pos2
    let (name, _) = decodeCStringAt(data, pos)
    msg.name = name
    PgOutputMessage(kind: pomkType, typeMsg: msg)
  of 'I': # Insert
    var msg = InsertMessage()
    msg.relationId = readInt32At(data, 1)
    let marker = char(readByteAt(data, 5)) # 'N' (new tuple marker)
    if marker != 'N':
      raise newException(PgProtocolError, "Unknown Insert tuple marker: " & marker)
    let (fields, _) = decodeTuple(data, 6)
    msg.newTuple = fields
    PgOutputMessage(kind: pomkInsert, insert: msg)
  of 'U': # Update
    var msg = UpdateMessage()
    msg.relationId = readInt32At(data, 1)
    var pos = 5
    let marker = char(readByteAt(data, pos))
    inc pos
    if marker == 'K' or marker == 'O':
      # Old key or old tuple included
      msg.keyKind = marker
      let (oldFields, nextPos) = decodeTuple(data, pos)
      msg.oldTuple = oldFields
      pos = nextPos
      let newMarker = char(readByteAt(data, pos)) # 'N' (new tuple marker)
      if newMarker != 'N':
        raise
          newException(PgProtocolError, "Unknown Update new tuple marker: " & newMarker)
      inc pos
    elif marker != 'N':
      raise newException(PgProtocolError, "Unknown Update tuple marker: " & marker)
    let (newFields, _) = decodeTuple(data, pos)
    msg.newTuple = newFields
    PgOutputMessage(kind: pomkUpdate, update: msg)
  of 'D': # Delete
    var msg = DeleteMessage()
    msg.relationId = readInt32At(data, 1)
    var pos = 5
    let marker = char(readByteAt(data, pos)) # 'K' (key) or 'O' (old tuple)
    if marker != 'K' and marker != 'O':
      raise newException(PgProtocolError, "Unknown Delete tuple marker: " & marker)
    msg.keyKind = marker
    inc pos
    let (fields, _) = decodeTuple(data, pos)
    msg.oldTuple = fields
    PgOutputMessage(kind: pomkDelete, delete: msg)
  of 'T': # Truncate
    var msg = TruncateMessage()
    let numRels = readInt32At(data, 1)
    msg.options = readByteAt(data, 5)
    var pos = 6
    # Each relation id is exactly 4 bytes; reject a count that cannot fit in the
    # remaining buffer before allocating, so a forged count can neither trigger
    # a huge allocation nor over-read in the loop below.
    if numRels < 0 or numRels.int > (data.len - pos) div 4:
      raise newException(PgProtocolError, "pgoutput Truncate: invalid relation count")
    msg.relationIds = newSeq[int32](numRels)
    for i in 0 ..< numRels:
      msg.relationIds[i] = readInt32At(data, pos)
      pos += 4
    PgOutputMessage(kind: pomkTruncate, truncate: msg)
  of 'M': # Message
    var msg = LogicalMessage()
    msg.flags = readByteAt(data, 1)
    msg.lsn = Lsn(cast[uint64](readInt64At(data, 2)))
    var pos = 10
    let (prefix, nextPos) = decodeCStringAt(data, pos)
    msg.prefix = prefix
    pos = nextPos
    let contentLen = readInt32At(data, pos)
    pos += 4
    msg.content = readBytesAt(data, pos, int(contentLen))
    PgOutputMessage(kind: pomkMessage, message: msg)
  else:
    raise newException(PgProtocolError, "Unknown pgoutput message type: " & msgType)

proc receivedEndLsn*(msg: XLogData): Lsn =
  ## End LSN of the WAL data actually contained in this message
  ## (``startLsn + len(data)``). Use this when acknowledging received data via
  ## ``sendStandbyStatus``; do not use ``walEnd``, which is the server's
  ## current WAL position and may point past data this message does not carry.
  let startLsn = uint64(msg.startLsn)
  let dataLen = uint64(msg.data.len)
  # Unsigned addition wraps silently instead of raising; check before adding.
  if dataLen > high(uint64) - startLsn:
    raise newException(
      PgProtocolError, "receivedEndLsn: startLsn + data.len overflows uint64"
    )
  Lsn(startLsn + dataLen)

proc decodePgOutput*(msg: XLogData): PgOutputMessage =
  ## Convenience: decode the pgoutput message from an XLogData's data field.
  parsePgOutputMessage(msg.data)

# Replication callback types

declareAsyncCallback(
  ReplicationCallback, proc(msg: ReplicationMessage): Future[void],
  "Callback invoked for each replication message during streaming.",
)

template makeReplicationCallback*(body: untyped): ReplicationCallback =
  ## Create a ``ReplicationCallback`` that works with both asyncdispatch and chronos.
  ## Inside ``body``, the current message is available as ``msg: ReplicationMessage``.
  ##
  ## Kept module-local: routing this through a shared template with an
  ## `untyped`/`typedesc` param for the parameter type trips asyncdispatch's
  ## `{.async.}` macro ("cannot use symbol of kind 'func' as a 'param'").
  block:
    when hasChronos:
      let r: ReplicationCallback = proc(
          msg {.inject.}: ReplicationMessage
      ) {.async: (raises: [CatchableError]).} =
        body
      r
    else:
      let r: ReplicationCallback = proc(msg {.inject.}: ReplicationMessage) {.async.} =
        body
      r

# Replication connection

proc replicationParamValue(mode: ReplicationMode): string {.inline.} =
  case mode
  of rmDatabase: "database"
  of rmPhysical: "true"

proc connectReplication*(
    config: ConnConfig, mode: ReplicationMode = rmDatabase
): Future[PgConnection] =
  ## Connect with ``replication`` param. ``rmPhysical`` allows only replication cmds.
  var cfg = config
  cfg.extraParams.add(("replication", replicationParamValue(mode)))
  connect(cfg)

proc connectReplication*(
    dsn: string, mode: ReplicationMode = rmDatabase
): Future[PgConnection] =
  ## DSN-string variant of ``connectReplication``. See the ``ConnConfig``
  ## overload for the meaning of ``mode``.
  var cfg = parseDsn(dsn)
  cfg.extraParams.add(("replication", replicationParamValue(mode)))
  connect(cfg)

proc parseTimelineId*(s: string): int32 =
  ## Parse the timeline id from an ``IDENTIFY_SYSTEM`` result row (text format).
  ## Converts a non-numeric value and an out-of-``int32``-range value into
  ## `PgTypeError` so callers stay under the ``except PgError`` contract.
  ## Range-check before narrowing: a bare ``parseInt(...).int32`` would raise
  ## ``RangeDefect`` (a Defect, outside ``PgError``) on an out-of-range value.
  pgTypeErrorOnValueError("IDENTIFY_SYSTEM returned a non-numeric timeline: " & s):
    let t = parseInt(s)
    if t < int(int32.low) or t > int(int32.high):
      raise newException(
        PgTypeError, "IDENTIFY_SYSTEM returned a timeline out of int32 range: " & s
      )
    t.int32

# Replication commands (via simple query protocol)

proc identifySystem*(
    conn: PgConnection, timeout: async_backend.Duration = ZeroDuration
): Future[SystemInfo] {.async.} =
  ## Execute ``IDENTIFY_SYSTEM`` and return system identification info.
  ##
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  let results = await conn.simpleQuery("IDENTIFY_SYSTEM", timeout)
  if results.len == 0 or results[0].rowCount == 0:
    raise newException(PgConnectionError, "IDENTIFY_SYSTEM returned no results")
  let qr = results[0]
  # Guard fixed-column access so a malformed server response surfaces as a
  # catchable PgConnectionError instead of an uncatchable IndexDefect from
  # `cellInfo`.
  if qr.fields.len < 3:
    raise newException(
      PgConnectionError,
      "IDENTIFY_SYSTEM returned " & $qr.fields.len & " columns, expected >= 3",
    )
  let row = initRow(qr.data, 0)
  var info = SystemInfo()
  info.systemId = row.getStr(0)
  info.timeline = parseTimelineId(row.getStr(1))
  info.xLogPos = parseLsn(row.getStr(2))
  # On physical replication connections (``replication=true``) the dbName
  # column is NULL because the session is not bound to a database.
  if qr.fields.len > 3 and not row.isNull(3):
    info.dbName = row.getStr(3)
  return info

proc decodeCreateSlotRow(qr: QueryResult): ReplicationSlotInfo =
  if qr.fields.len < 2:
    raise newException(
      PgConnectionError,
      "CREATE_REPLICATION_SLOT returned " & $qr.fields.len & " columns, expected >= 2",
    )
  let row = initRow(qr.data, 0)
  result.slotName = row.getStr(0)
  result.consistentPoint = parseLsn(row.getStr(1))
  if qr.fields.len > 2 and not row.isNull(2):
    result.snapshotName = row.getStr(2)
  if qr.fields.len > 3 and not row.isNull(3):
    result.outputPlugin = row.getStr(3)

proc createReplicationSlot*(
    conn: PgConnection,
    slotName: string,
    plugin: string = "pgoutput",
    temporary: bool = false,
    timeout: async_backend.Duration = ZeroDuration,
): Future[ReplicationSlotInfo] {.async.} =
  ## Create a logical replication slot. Returns slot info including the consistent point LSN.
  ##
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  var sql = "CREATE_REPLICATION_SLOT " & quoteIdentifier(slotName)
  if temporary:
    sql.add(" TEMPORARY")
  sql.add(" LOGICAL " & quoteIdentifier(plugin))

  let results = await conn.simpleQuery(sql, timeout)
  if results.len == 0 or results[0].rowCount == 0:
    raise newException(PgConnectionError, "CREATE_REPLICATION_SLOT returned no results")
  return decodeCreateSlotRow(results[0])

proc dropReplicationSlot*(
    conn: PgConnection,
    slotName: string,
    wait: bool = false,
    timeout: async_backend.Duration = ZeroDuration,
): Future[void] {.async.} =
  ## Drop a replication slot.
  ##
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  var sql = "DROP_REPLICATION_SLOT " & quoteIdentifier(slotName)
  if wait:
    sql.add(" WAIT")
  discard await conn.simpleQuery(sql, timeout)

proc readReplicationSlot*(
    conn: PgConnection, slotName: string, timeout: async_backend.Duration = ZeroDuration
): Future[ReplicationSlotInfo] {.async.} =
  ## Read information about an existing replication slot.
  ##
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  let results = await conn.simpleQuery(
    "READ_REPLICATION_SLOT " & quoteIdentifier(slotName), timeout
  )
  if results.len == 0 or results[0].rowCount == 0:
    raise newException(PgConnectionError, "READ_REPLICATION_SLOT returned no results")
  let qr = results[0]
  if qr.fields.len < 2:
    raise newException(
      PgConnectionError,
      "READ_REPLICATION_SLOT returned " & $qr.fields.len & " columns, expected >= 2",
    )
  let row = initRow(qr.data, 0)
  var info = ReplicationSlotInfo()
  # READ_REPLICATION_SLOT returns: slot_type, restart_lsn, restart_tli
  # But the column layout depends on PG version. We handle common case.
  info.slotName = slotName
  if not row.isNull(1):
    info.consistentPoint = parseLsn(row.getStr(1))
  return info

proc timelineHistory*(
    conn: PgConnection, timeline: int32, timeout: async_backend.Duration = ZeroDuration
): Future[TimelineHistory] {.async.} =
  ## Execute ``TIMELINE_HISTORY``. Raises ``ValueError`` if ``timeline <=0``.
  ## On timeout the connection is marked ``csClosed`` (protocol out of sync), so
  ## a caller catching ``PgTimeoutError`` must reconnect, not retry in place.
  if timeline <= 0:
    raise newException(ValueError, "timeline must be > 0, got " & $timeline)
  let results = await conn.simpleQuery("TIMELINE_HISTORY " & $timeline, timeout)
  if results.len == 0 or results[0].rowCount == 0:
    raise newException(PgConnectionError, "TIMELINE_HISTORY returned no results")
  let qr = results[0]
  if qr.fields.len < 2:
    raise newException(
      PgConnectionError,
      "TIMELINE_HISTORY returned " & $qr.fields.len & " columns, expected >= 2",
    )
  let row = initRow(qr.data, 0)
  var info = TimelineHistory()
  if not row.isNull(0):
    info.filename = row.getStr(0)
  if not row.isNull(1):
    info.content = row.getBytes(1)
  return info

# Replication streaming

proc parseReplicationMessage*(copyData: sink seq[byte]): ReplicationMessage =
  ## Parse a CopyData payload into a ReplicationMessage. Takes ownership of
  ## ``copyData`` so the XLogData path can reuse the incoming buffer for
  ## ``xlogData.data`` instead of slicing into a fresh allocation.
  if copyData.len == 0:
    raise newException(PgProtocolError, "Empty replication CopyData")
  let kind = char(copyData[0])
  case kind
  of 'w': # XLogData
    if copyData.len < 25:
      raise newException(PgProtocolError, "XLogData message too short")
    var xlog = XLogData()
    xlog.startLsn = Lsn(cast[uint64](decodeInt64(copyData, 1)))
    xlog.walEnd = Lsn(cast[uint64](decodeInt64(copyData, 9)))
    xlog.sendTime = decodeInt64(copyData, 17)
    const dataStart = 25
    if copyData.len > dataStart:
      # Reuse the incoming buffer: strip the 25-byte header in place instead
      # of allocating a fresh seq for the payload slice.
      xlog.data = move(copyData)
      let newLen = xlog.data.len - dataStart
      moveMem(addr xlog.data[0], addr xlog.data[dataStart], newLen)
      xlog.data.setLen(newLen)
    ReplicationMessage(kind: rmkXLogData, xlogData: xlog)
  of 'k': # Primary Keepalive
    if copyData.len < 18:
      raise newException(PgProtocolError, "Primary Keepalive message too short")
    var ka = PrimaryKeepalive()
    ka.walEnd = Lsn(cast[uint64](decodeInt64(copyData, 1)))
    ka.sendTime = decodeInt64(copyData, 9)
    ka.replyRequested = copyData[17] != 0
    ReplicationMessage(kind: rmkPrimaryKeepalive, keepalive: ka)
  else:
    raise newException(PgProtocolError, "Unknown replication message type: " & kind)

proc checkReplicating(conn: PgConnection, op: string) =
  ## ``csReplicating`` guard for a mid-stream operation. A connection the
  ## application closed itself, or one simply never put into a stream, is a
  ## caller-state error (``PgStateError``); only a lost transport is a
  ## ``PgConnectionError``, so a reconnect loop does not fire on a shutdown the
  ## application requested.
  # ``closedByUser`` first, as ``checkReady`` does: ``close()`` sets it while the
  # connection is still ``csReplicating``.
  conn.checkNotClosed()
  if conn.state == csReplicating:
    return
  raise newException(
    PgStateError,
    op & ": connection is not in replicating state (state: " & $conn.state & ")",
  )

proc sendCopyData*(conn: PgConnection, data: openArray[byte]): Future[void] =
  ## Send CopyData during ``csReplicating``. Raises ``PgStateError`` (not
  ## replicating) / ``PgConnectionError`` (connection lost) / ``PgTypeError``
  ## synchronously before first suspension. ``data`` is encoded into the frame
  ## there too, so the caller's buffer need not outlive the returned ``Future``.
  conn.checkReplicating("sendCopyData")
  var buf: seq[byte]
  encodeCopyData(buf, data)
  conn.sendMsg(buf)

proc sendStandbyStatusRaw(
    conn: PgConnection, receiveLsn, flushLsn, applyLsn: Lsn, replyRequested: bool
): Future[void] {.async.} =
  ## Encode and send a Standby Status Update with the given receive/flush/apply
  ## LSNs verbatim — no ``InvalidLsn`` defaulting. This is the single place the
  ## wire encoding lives; the public ``sendStandbyStatus`` (which applies the
  ## up-to-receive defaulting) and ``sendConfirmedStatus`` (which sends the
  ## confirmed position verbatim) both route through it. Callers are responsible
  ## for the ``csReplicating`` guard.
  let msg = encodeStandbyStatusUpdate(
    receiveLsn.toInt64,
    flushLsn.toInt64,
    applyLsn.toInt64,
    currentPgTimestamp(),
    if replyRequested: 1'u8 else: 0'u8,
  )
  await conn.sendMsg(msg)

proc sendStandbyStatus*(
    conn: PgConnection,
    receiveLsn: Lsn,
    flushLsn: Lsn = InvalidLsn,
    applyLsn: Lsn = InvalidLsn,
    replyRequested: bool = false,
): Future[void] {.async.} =
  ## Send Standby Status Update. ``InvalidLsn`` defaults up to ``receiveLsn``.
  ## Raises ``PgStateError`` unless the connection is ``csReplicating``, or
  ## ``PgConnectionError`` when the connection was lost.
  conn.checkReplicating("sendStandbyStatus")
  let flushVal = if flushLsn == InvalidLsn: receiveLsn else: flushLsn
  let applyVal = if applyLsn == InvalidLsn: receiveLsn else: applyLsn
  await conn.sendStandbyStatusRaw(receiveLsn, flushVal, applyVal, replyRequested)

proc confirmedFlushLsn*(conn: PgConnection): Lsn {.inline.} =
  ## Confirmed flush LSN for current stream, or ``InvalidLsn`` outside stream.
  # ``closedByUser`` leads, matching ``checkReplicating``: ``close()`` sets it
  # while still ``csReplicating``, and the stream is over from that point on.
  if conn.closedByUser or conn.state != csReplicating:
    return InvalidLsn
  Lsn(conn.replConfirmedFlushLsn())

proc confirmFlushed*(conn: PgConnection, lsn: Lsn): bool =
  ## Confirm WAL up to ``lsn`` as durable. Clamped to received WAL, monotonic.
  ## Returns true if advanced. Must be in ``csReplicating``.
  conn.checkReplicating("confirmFlushed")
  # Clamp to received WAL: durably-persisted WAL can never exceed what was
  # received. Clamping (rather than raising) keeps automatic replies from
  # emitting flush ahead of receive without letting an out-of-range LSN — e.g.
  # the readily-available ``walEnd`` — throw out of the callback and strand the
  # connection in ``csReplicating``. The raw helper in pg_connection/types
  # performs the clamp and the monotonic advance in one place.
  return conn.confirmReplFlushed(lsn.toUInt64)

proc sendConfirmedStatus(conn: PgConnection, receiveLsn: Lsn): Future[void] {.async.} =
  ## Send a Standby Status Update carrying ``receiveLsn`` in the *receive* field
  ## (which resets ``wal_sender_timeout`` on the server) and the
  ## ``confirmFlushed`` position in flush/apply. The confirmed position is sent
  ## verbatim — it is the stream's ``startLsn`` until ``confirmFlushed`` advances
  ## it, so when nothing has been confirmed and ``startLsn`` was left at its
  ## default ``InvalidLsn`` it is ``0/0``, which PostgreSQL reads as "position
  ## unknown" and will not move the slot backwards. Either way flush never
  ## advances past WAL the callback has not yet confirmed durable. Used by the
  ## automatic keepalive reply and by ``stopReplication``.
  ##
  ## Only valid while ``csReplicating``, where ``confirmedFlushLsn`` is bounded
  ## by received WAL (see ``confirmFlushed``), so flush never exceeds receive.
  ## Calling this outside an active replication stream raises ``PgStateError``.
  conn.checkReplicating("sendConfirmedStatus")
  let flushLsn = conn.confirmedFlushLsn
  await conn.sendStandbyStatusRaw(
    receiveLsn, flushLsn, flushLsn, replyRequested = false
  )

proc resetReplLsnTracking(conn: PgConnection, startLsn: Lsn) =
  ## Reset the per-stream confirmed-flush and max-received positions to the
  ## resume point at the start of a stream, so a reused connection never inherits
  ## a stale value from a previous stream. The confirmed-flush position then
  ## advances only via ``confirmFlushed``; the max-received position advances as
  ## ``XLogData`` arrives and bounds what ``confirmFlushed`` will accept.
  conn.initReplLsnTracking(startLsn.toUInt64)
  conn.replCopyDoneSent = false

proc replFillRecvBuf(
    conn: PgConnection,
    statusInterval: async_backend.Duration,
    lastStatusSent: Moment,
    pendingRead: Future[void],
): Future[Future[void]] {.async.} =
  ## Wait for more replication data, but wake early enough that the caller can
  ## emit a proactive Standby Status Update when ``statusInterval`` is set.
  ##
  ## ``pendingRead`` carries a single in-flight read across calls (``nil`` when
  ## none is outstanding). The updated read is returned: still pending after a
  ## timed wake, or ``nil`` once it has been consumed. The caller threads the
  ## returned value back in on the next call.
  ##
  ## With ``statusInterval == ZeroDuration`` (the default) this blocks until data
  ## arrives, exactly like a bare ``fillRecvBuf``.
  ##
  ## With a positive ``statusInterval`` under **chronos**, a single background read
  ## is raced against a timer sized to the time left until the next status update
  ## is due. On a timer wake the read is **left in flight** (never cancelled) and
  ## resumed on the next call: cancelling an in-flight transport read and then
  ## starting another races chronos' asynchronous cancellation (see
  ## ``RecvWatch.cancel``) and can surface as a "Read operation already pending"
  ## ``AsyncStreamReadError``. ``fillRecvBufDetached`` commits its bytes to ``recvBuf``
  ## only when awaited, so a read that completes during the timed wait is neither
  ## lost nor double-counted. Under **asyncdispatch** there is no timer-bounded
  ## wake — a timed read cannot be cancelled, and the abandoned read would consume
  ## and drop bytes, desyncing the stream — so it falls back to an unbounded read.
  ## The caller still emits status updates opportunistically after each received
  ## message, which covers a busy stream (where WAL actually accumulates); a fully
  ## idle asyncdispatch stream sends nothing until the next message arrives.
  if statusInterval <= ZeroDuration:
    await conn.fillRecvBuf()
    return nil
  when hasChronos:
    var read = pendingRead
    # A locally spawned read isn't visible to the caller yet: chronos ``race``
    # does not cancel its children, and a cancel here unwinds before we can
    # return it — so drop it explicitly. A passed-in read is already tracked by
    # the caller's cleanup.
    let readIsLocal = read == nil
    if readIsLocal:
      read = conn.fillRecvBufDetached()
    if not read.finished:
      let sinceLast = Moment.now() - lastStatusSent
      let remaining =
        if sinceLast >= statusInterval:
          async_backend.milliseconds(1)
        else:
          statusInterval - sinceLast
      let timer = sleepAsync(remaining)
      try:
        discard await race(read, timer)
      except CancelledError as e:
        if readIsLocal and not read.finished:
          read.cancelSoon()
        raise e
      finally:
        cancelTimer(timer)
    if read.finished:
      await read # commit bytes to recvBuf (or re-raise a transport failure)
      return nil
    return read # timed wake: read still in flight, resume it on the next call
  else:
    await conn.fillRecvBuf()
    return nil

proc maybeSendPeriodicStatus(
    conn: PgConnection,
    autoKeepaliveReply: bool,
    statusInterval: async_backend.Duration,
    lastStatusSent: Moment,
): Future[Moment] {.async.} =
  ## Emit a proactive Standby Status Update if ``statusInterval`` has elapsed
  ## since the last one, so ``confirmed_flush_lsn`` advances (and
  ## ``wal_sender_timeout`` resets) even when the server never requests a reply —
  ## e.g. a server configured with ``wal_sender_timeout = 0``. The update reports
  ## the highest received LSN as receive and the ``confirmFlushed`` position as
  ## flush/apply, identical to the automatic keepalive reply, so it never advances
  ## flush past WAL the callback has confirmed durable. Returns the timestamp to
  ## record as the new ``lastStatusSent`` (unchanged when nothing was sent).
  ##
  ## Only active together with ``autoKeepaliveReply``: under manual reply
  ## management the caller owns the cadence and the reported LSNs via
  ## ``sendStandbyStatus``.
  if not autoKeepaliveReply or statusInterval <= ZeroDuration:
    return lastStatusSent
  if conn.state != csReplicating:
    return lastStatusSent
  if Moment.now() - lastStatusSent < statusInterval:
    return lastStatusSent
  await sendConfirmedStatus(conn, Lsn(conn.replMaxReceivedLsn()))
  return Moment.now()

proc handleReplicationData(
    conn: PgConnection,
    copyData: sink seq[byte],
    autoKeepaliveReply: bool,
    callback: ReplicationCallback,
    lastStatusSent: Moment,
): Future[Moment] {.async.} =
  ## Process one CopyData frame from a replication stream: parse it, advance the
  ## received-WAL position on ``XLogData`` (the single source of truth read by
  ## ``confirmFlushed`` and the auto-reply), emit an automatic keepalive reply on
  ## a ``PrimaryKeepalive`` with ``replyRequested`` when ``autoKeepaliveReply`` is
  ## set, then invoke the user ``callback``. Shared by ``startReplication`` and
  ## ``startPhysicalReplication`` so the received-tracking and auto-reply logic
  ## lives in exactly one place.
  ##
  ## Returns the timestamp to record as ``lastStatusSent``; it is updated when
  ## an automatic keepalive reply is sent so that ``statusInterval`` tracks the
  ## last time the server saw a Standby Status Update, preventing duplicate
  ## proactive updates.
  var newLastStatusSent = lastStatusSent
  let replMsg = parseReplicationMessage(move(copyData))
  case replMsg.kind
  of rmkXLogData:
    let received = replMsg.xlogData.receivedEndLsn
    discard conn.updateReplMaxReceivedLsn(received.toUInt64)
  of rmkPrimaryKeepalive:
    if autoKeepaliveReply and replMsg.keepalive.replyRequested:
      await sendConfirmedStatus(conn, Lsn(conn.replMaxReceivedLsn()))
      newLastStatusSent = Moment.now()
  await callback(replMsg)
  return newLastStatusSent

proc invalidateAbandonedStream(conn: PgConnection) =
  ## Poison a connection whose CopyBoth replication stream was torn down
  ## mid-flight — most often because the user ``callback`` raised, but also any
  ## other failure that unwinds the streaming loop while the stream is still
  ## open. The half-finished CopyBoth exchange leaves the protocol stream out of
  ## sync (the server is still streaming WAL the client will never drain), so the
  ## connection cannot be reused; mark it ``csClosed`` so the next operation
  ## fails fast and a pool discards it.
  ##
  ## Without this the connection would be stranded in ``csBusy`` or
  ## ``csReplicating``: every later call would raise a misleading ``PgStateError``
  ## ("connection is in use") for an apparently-live stream when the stream is in
  ## fact dead, and the only recovery is to reconnect and resume (see
  ## ``examples/replication.nim``). A clean stop (CopyDone -> ReadyForQuery) and a
  ## server-side error followed by ReadyForQuery both return the connection to
  ## ``csReady`` first, and the I/O helpers (``fillRecvBuf`` / ``sendMsg``) mark
  ## ``csClosed`` themselves on a dead socket — so only a still-``csBusy`` state
  ## (START_REPLICATION issued but CopyBothResponse never arrived, sub-cases where
  ## the raiser did not mark ``csClosed``) or ``csReplicating`` state (torn down
  ## mid-stream), the stranded cases, are changed here.
  if conn.state in {csBusy, csReplicating}:
    conn.state = csClosed

proc runReplicationStream(
    conn: PgConnection,
    startLsn: Lsn,
    autoKeepaliveReply: bool,
    statusInterval: async_backend.Duration,
    callback: ReplicationCallback,
    context: string,
): Future[void] {.async.} =
  ## Shared replication stream body. Caller must have already sent the
  ## ``START_REPLICATION`` query. ``context`` appears in error messages
  ## (e.g. ``"replication"`` / ``"physical replication"``).
  var queryError: ref PgQueryError

  # Register the poison-on-abandon defer BEFORE waitCopyBoth so a raise during
  # the CopyBothResponse wait (state still csBusy) is also poisoned, not just a
  # mid-stream failure once csReplicating.
  defer:
    conn.invalidateAbandonedStream()

  block waitCopyBoth:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyBothResponse:
          conn.state = csReplicating
          break waitCopyBoth
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          raise newException(
            PgConnectionError,
            "START_REPLICATION " & context & " ended without CopyBothResponse",
          )
        else:
          discard
      await conn.fillRecvBuf()

  conn.resetReplLsnTracking(startLsn)

  var lastStatusSent = Moment.now()
  var pendingRead: Future[void] = nil
  when hasChronos:
    defer:
      if pendingRead != nil and not pendingRead.finished:
        pendingRead.cancelSoon()

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        var msg = opt.get
        case msg.kind
        of bmkCopyData:
          lastStatusSent = await conn.handleReplicationData(
            move(msg.copyData), autoKeepaliveReply, callback, lastStatusSent
          )
        of bmkCopyDone:
          # Mirror only on server-initiated stop (walsender timeout,
          # pg_terminate_backend, slot drop). If the client already sent
          # CopyDone via stopReplication, a second one would land after the
          # server left COPY mode -> "invalid frontend message type".
          if not conn.replCopyDoneSent:
            try:
              await sendConfirmedStatus(conn, Lsn(conn.replMaxReceivedLsn()))
            except CancelledError as e:
              raise e
            except CatchableError:
              discard
            conn.replCopyDoneSent = true
            await conn.sendMsg(@copyDoneMsg)
          break recvLoop
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          return
        else:
          discard
      lastStatusSent = await conn.maybeSendPeriodicStatus(
        autoKeepaliveReply, statusInterval, lastStatusSent
      )
      if conn.state == csClosed:
        conn.raiseClosedConnection("Connection closed during " & context)
      # Without autoKeepaliveReply, lastStatusSent never advances, so a timer
      # race here would rearm every ~1 ms.
      let effectiveInterval = if autoKeepaliveReply: statusInterval else: ZeroDuration
      pendingRead =
        await conn.replFillRecvBuf(effectiveInterval, lastStatusSent, pendingRead)
      lastStatusSent = await conn.maybeSendPeriodicStatus(
        autoKeepaliveReply, statusInterval, lastStatusSent
      )

  block drainLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.state = csReady
          if queryError != nil:
            raise queryError
          break drainLoop
        else:
          discard
      await conn.fillRecvBuf()

proc startReplication*(
    conn: PgConnection,
    slotName: string,
    startLsn: Lsn = InvalidLsn,
    options: seq[(string, string)] = @[],
    autoKeepaliveReply: bool = true,
    statusInterval: async_backend.Duration = ZeroDuration,
    callback: ReplicationCallback,
): Future[void] {.async.} =
  ## Begin logical replication. Callback invoked per message. Use
  ## ``confirmFlushed`` for flush tracking; or set ``autoKeepaliveReply=false``
  ## and use ``sendStandbyStatus`` manually.
  ##
  ## Returns on server ``CopyDone`` or connection close. To stop from the client
  ## side, call ``stopReplication`` from the callback (or a concurrent task).
  ##
  ## Errors poison connection. Track LSN for resume. A failing auto-reply
  ## propagates too, and the callback is *not* invoked for that keepalive.
  ## Options appended verbatim —
  ## quote untrusted input. Raises ``PgConnectionError`` (closed) /
  ## ``PgStateError`` (busy) unless ``csReady``, and ``ValueError`` for a
  ## ``proto_version`` other than ``1`` in ``options``: the bundled pgoutput
  ## decoder supports v1 only. ``publication_names`` without an explicit
  ## ``proto_version`` adds ``proto_version = '1'`` to the generated command, so
  ## a server-side default bump cannot outrun that decoder.
  ##
  ## ``statusInterval`` (``ZeroDuration`` = off) sends a proactive Standby Status
  ## Update at least that often — receive = highest received, flush/apply =
  ## ``confirmFlushed`` — so the slot advances on a server that never requests a
  ## reply (``wal_sender_timeout = 0``). Honoured only with
  ## ``autoKeepaliveReply``; under asyncdispatch it fires only while messages are
  ## flowing, so a fully idle stream sends nothing until the next message.
  ##
  ## **Synchronous standbys:** the auto-reply reports receive and flush/apply
  ## separately, so a consumer in ``synchronous_standby_names`` that never calls
  ## ``confirmFlushed`` keeps ``wal_sender_timeout`` reset via the receive field
  ## yet never advances flush — the primary's ``COMMIT``s then block indefinitely.
  # Reject unsupported proto_version pre-flight so the failure is a plain input
  # error rather than a mid-stream decode break.
  var hasProtoVersion = false
  var hasPublicationNames = false
  for (k, v) in options:
    if k.cmpIgnoreCase("proto_version") == 0:
      hasProtoVersion = true
      let pv = v.strip(chars = {'\'', '"', ' ', '\t'})
      if pv.len > 0 and pv != "1":
        raise newException(
          ValueError,
          "Unsupported pgoutput proto_version " & v &
            ": the bundled decoder supports proto_version 1 only",
        )
    elif k.cmpIgnoreCase("publication_names") == 0:
      hasPublicationNames = true

  conn.checkReady()

  # publication_names => pgoutput; pin proto_version defensively against a
  # future server-side default bump past 1.
  var effectiveOptions = options
  if hasPublicationNames and not hasProtoVersion:
    effectiveOptions.add(("proto_version", "'1'"))

  # Build START_REPLICATION command
  var sql =
    "START_REPLICATION SLOT " & quoteIdentifier(slotName) & " LOGICAL " & $startLsn
  if effectiveOptions.len > 0:
    sql.add(" (")
    for i, (k, v) in effectiveOptions:
      if i > 0:
        sql.add(", ")
      for j, c in k:
        if j == 0:
          if c notin {'a' .. 'z', 'A' .. 'Z', '_'}:
            raise newException(ValueError, "Invalid replication option key: " & k)
        else:
          if c notin {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'}:
            raise newException(ValueError, "Invalid replication option key: " & k)
      sql.add(k)
      if v.len > 0:
        sql.add(" " & v)
    sql.add(")")

  let msg = encodeQuery(sql)
  conn.state = csBusy
  await conn.sendMsg(msg)
  await runReplicationStream(
    conn, startLsn, autoKeepaliveReply, statusInterval, callback, "replication"
  )

proc stopReplication*(conn: PgConnection): Future[void] {.async.} =
  ## Terminate replication. Flushes confirmed position before CopyDone.
  ## Raises ``PgStateError`` unless the connection is ``csReplicating``, or
  ## ``PgConnectionError`` when the connection was lost.
  conn.checkReplicating("stopReplication")
  await sendConfirmedStatus(conn, Lsn(conn.replMaxReceivedLsn()))
  conn.replCopyDoneSent = true
  await conn.sendMsg(@copyDoneMsg)

proc startPhysicalReplication*(
    conn: PgConnection,
    startLsn: Lsn,
    slotName: string = "",
    timeline: int32 = 0,
    autoKeepaliveReply: bool = true,
    statusInterval: async_backend.Duration = ZeroDuration,
    callback: ReplicationCallback,
): Future[void] {.async.} =
  ## Physical replication streaming. Callback per message, raw WAL in ``XLogData``.
  ## Raises ``PgConnectionError`` (closed) / ``PgStateError`` (busy) unless
  ## ``csReady``. Error handling matches ``startReplication``: a callback
  ## exception or any other mid-stream failure poisons the connection (marked
  ## closed) and propagates, so reconnect and resume from the last LSN tracked.
  ##
  ## ``slotName = ""`` streams without a slot. A non-zero ``timeline`` is appended
  ## as ``TIMELINE n``, so the server aborts the stream if it advanced past that
  ## timeline. ``statusInterval`` behaves as on ``startReplication``.
  ##
  ## On a timeline switch the server may send a result set describing the next
  ## timeline between ``CopyDone`` and ``ReadyForQuery``; this proc drains and
  ## discards it — re-issue ``IDENTIFY_SYSTEM`` if you need that information.
  ##
  ## **Synchronous standbys:** the flush LSN governs how much WAL the primary may
  ## recycle, so a standby in ``synchronous_standby_names`` that relies on the
  ## auto-reply must call ``confirmFlushed`` (or reply manually) or the primary's
  ## ``COMMIT``s block waiting on a flush position that never advances.
  conn.checkReady()

  var sql = "START_REPLICATION"
  if slotName.len > 0:
    sql.add(" SLOT " & quoteIdentifier(slotName))
  sql.add(" PHYSICAL " & $startLsn)
  if timeline > 0:
    sql.add(" TIMELINE " & $timeline)

  let msg = encodeQuery(sql)
  conn.state = csBusy
  await conn.sendMsg(msg)
  await runReplicationStream(
    conn, startLsn, autoKeepaliveReply, statusInterval, callback, "physical replication"
  )
