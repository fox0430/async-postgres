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
    hasOldTuple*: bool ## True if old key/full row is included
    keyKind*: char
      ## When hasOldTuple is true: 'K' if oldTuple holds only the replica
      ## identity key, 'O' if it holds the full old row (REPLICA IDENTITY FULL).
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

  pgEpochOffset* = 946_684_800'i64
    ## Seconds between Unix epoch (1970-01-01) and PostgreSQL epoch (2000-01-01).

  MaxRelationColumns = 1600
    ## PostgreSQL's max columns per table (``MaxHeapAttributeNumber``).
    ## pgoutput's column-count wire field can never exceed this in practice.

proc `==`*(a, b: Lsn): bool {.borrow.}
proc `<`*(a, b: Lsn): bool {.borrow.}
proc `<=`*(a, b: Lsn): bool {.borrow.}
proc `>`*(a, b: Lsn): bool {.inline.} =
  b < a

proc `>=`*(a, b: Lsn): bool {.inline.} =
  b <= a

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
  let now = epochTime()
  int64((now - float64(pgEpochOffset)) * 1_000_000.0)

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
  if offset >= buf.len:
    raise newException(PgProtocolError, "decodeCStringAt: offset past end of buffer")
  var i = offset
  while i < buf.len and buf[i] != 0:
    inc i
  if i >= buf.len:
    raise newException(PgProtocolError, "decodeCStringAt: missing null terminator")
  let slen = i - offset
  let s = readString(buf, offset, slen)
  inc i # skip null
  (s, i)

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
      msg.hasOldTuple = true
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

when hasChronos:
  type ReplicationCallback* = proc(msg: ReplicationMessage): Future[void] {.
    async: (raises: [CatchableError]), gcsafe
  .} ## Callback invoked for each replication message during streaming.

else:
  type ReplicationCallback* = proc(msg: ReplicationMessage): Future[void] {.gcsafe.}
    ## Callback invoked for each replication message during streaming.

template makeReplicationCallback*(body: untyped): ReplicationCallback =
  ## Create a ``ReplicationCallback`` that works with both asyncdispatch and chronos.
  ## Inside ``body``, the current message is available as ``msg: ReplicationMessage``.
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
  ## Connect to PostgreSQL with the ``replication`` startup parameter set.
  ## ``rmDatabase`` enables logical replication commands (IDENTIFY_SYSTEM,
  ## CREATE_REPLICATION_SLOT, START_REPLICATION ... LOGICAL, etc.) against
  ## the chosen database. ``rmPhysical`` opens a physical replication
  ## connection (``replication=true``); only replication commands work — no
  ## SQL on user databases is permitted.
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
  let qr = results[0]
  let row = initRow(qr.data, 0)
  var info = ReplicationSlotInfo()
  info.slotName = row.getStr(0)
  info.consistentPoint = parseLsn(row.getStr(1))
  if qr.fields.len > 2:
    info.snapshotName = row.getStr(2)
  if qr.fields.len > 3:
    info.outputPlugin = row.getStr(3)
  return info

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
  ## Execute ``TIMELINE_HISTORY <tli>`` and return the history file metadata
  ## plus its raw contents. Required when a physical standby needs to follow
  ## a timeline switch on the primary.
  ##
  ## On timeout, the connection is marked csClosed (protocol out of sync).
  if timeline <= 0:
    raise newException(ValueError, "timeline must be > 0, got " & $timeline)
  let results = await conn.simpleQuery("TIMELINE_HISTORY " & $timeline, timeout)
  if results.len == 0 or results[0].rowCount == 0:
    raise newException(PgConnectionError, "TIMELINE_HISTORY returned no results")
  let qr = results[0]
  let row = initRow(qr.data, 0)
  var info = TimelineHistory()
  if not row.isNull(0):
    info.filename = row.getStr(0)
  if not row.isNull(1):
    info.content = row.getBytes(1)
  return info

# Replication streaming

proc parseReplicationMessage*(copyData: openArray[byte]): ReplicationMessage =
  ## Parse a CopyData payload into a ReplicationMessage.
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
    let dataStart = 25
    if copyData.len > dataStart:
      xlog.data = copyData[dataStart ..< copyData.len]
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

proc sendCopyData*(conn: PgConnection, data: openArray[byte]): Future[void] =
  ## Send a raw CopyData frame to the server during a CopyBoth stream
  ## (i.e. while the connection is in ``csReplicating``). Useful for protocols
  ## layered on top of CopyBoth — for example, physical replication
  ## acknowledgements or custom replication plugins that exchange messages
  ## the library does not know about. For Standby Status Updates, prefer
  ## ``sendStandbyStatus`` which builds the payload for you.
  ##
  ## ``data`` is encoded into a CopyData frame synchronously *before* the
  ## first async suspension, so the caller's buffer does not need to outlive
  ## the returned ``Future``.
  if conn.state != csReplicating:
    raise newException(
      PgConnectionError,
      "sendCopyData: connection is not in replicating state (state: " & $conn.state & ")",
    )
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
  ## Send a Standby Status Update to the server during replication streaming.
  ## Must be called while the connection is in ``csReplicating`` state.
  ##
  ## When ``flushLsn``/``applyLsn`` are left at ``InvalidLsn`` (``0/0``) they
  ## default *up to* ``receiveLsn`` — convenient for callers that ACK received
  ## data eagerly. Pass an explicit ``flushLsn``/``applyLsn`` to report a
  ## position behind ``receiveLsn`` (e.g. only what the callback has durably
  ## flushed). The automatic keepalive reply does not use this proc; it sends
  ## the confirmed-flush position verbatim via an internal path so it never
  ## inflates flush to merely-received WAL.
  if conn.state != csReplicating:
    raise newException(
      PgConnectionError,
      "sendStandbyStatus: connection is not in replicating state (state: " & $conn.state &
        ")",
    )
  let flushVal = if flushLsn == InvalidLsn: receiveLsn else: flushLsn
  let applyVal = if applyLsn == InvalidLsn: receiveLsn else: applyLsn
  await conn.sendStandbyStatusRaw(receiveLsn, flushVal, applyVal, replyRequested)

proc confirmedFlushLsn*(conn: PgConnection): Lsn {.inline.} =
  ## Highest LSN the application has confirmed durably flushed for the current
  ## replication stream via ``confirmFlushed``. Initialised to the stream's
  ## ``startLsn`` by ``startReplication`` / ``startPhysicalReplication``; this is
  ## the flush/apply position carried by automatic keepalive replies.
  ##
  ## Only meaningful during an active stream: outside ``csReplicating`` (before a
  ## stream starts or after it ends) this returns ``InvalidLsn`` (``0/0``) rather
  ## than a stale value left over from a previous stream.
  if conn.state != csReplicating:
    return InvalidLsn
  Lsn(conn.replConfirmedFlushLsn())

proc confirmFlushed*(conn: PgConnection, lsn: Lsn): bool =
  ## Record that received WAL up to and including ``lsn`` has been durably
  ## persisted by the application, so automatic keepalive replies (see
  ## ``autoKeepaliveReply`` on ``startReplication``) report it as the flush/apply
  ## position and let the server advance ``confirmed_flush_lsn`` and recycle WAL.
  ##
  ## Call this from the replication callback *after* the received changes are
  ## durable. Until you do, the automatic reply acknowledges only *receipt* (the
  ## receive LSN), never flush — so a crash re-streams the unprocessed WAL,
  ## giving at-least-once delivery. Calls that would move the confirmed position
  ## backwards are ignored, so duplicate or out-of-order confirmations are safe.
  ##
  ## ``lsn`` is clamped to the WAL actually received: you cannot have durably
  ## persisted WAL you have not yet received, so an ``lsn`` beyond the highest
  ## ``XLogData.receivedEndLsn`` observed confirms only up to that received
  ## position (passing ``walEnd`` — which runs ahead of the data this message
  ## carries — therefore confirms received WAL rather than over-advancing).
  ## Because of this clamp the confirmed position can never exceed received WAL,
  ## so automatic replies never emit a flush ahead of receive, and the call never
  ## raises on an out-of-range LSN (an uncaught raise from the callback would
  ## strand the stream). Must be called while the connection is ``csReplicating``
  ## (i.e. from the replication callback); calling it outside an active stream
  ## raises ``PgConnectionError``.
  ##
  ## Returns ``true`` when the confirmed-flush position actually moved forward
  ## (after clamping and the monotonic guard). ``false`` means the request was
  ## ignored because it was behind the current confirmed position.
  if conn.state != csReplicating:
    raise newException(
      PgConnectionError,
      "confirmFlushed: connection is not in replicating state (state: " & $conn.state &
        ")",
    )
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
  ## Calling this outside an active replication stream raises ``PgConnectionError``.
  if conn.state != csReplicating:
    raise newException(
      PgConnectionError,
      "sendConfirmedStatus: connection is not in replicating state (state: " &
        $conn.state & ")",
    )
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
    if read == nil:
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
    copyData: seq[byte],
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
  let replMsg = parseReplicationMessage(copyData)
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
  ## Without this the connection would be stranded in ``csReplicating``: every
  ## later call would raise a misleading ``PgStateError`` ("connection is in
  ## use") for an apparently-live stream when the stream is in fact dead, and the
  ## only recovery is to reconnect and resume (see ``examples/replication.nim``).
  ## A clean stop (CopyDone -> ReadyForQuery) and a server-side error followed by
  ## ReadyForQuery both return the connection to ``csReady`` first, and the I/O
  ## helpers (``fillRecvBuf`` / ``sendMsg``) mark ``csClosed`` themselves on a
  ## dead socket — so only a still-``csReplicating`` state, the stranded case, is
  ## changed here.
  if conn.state == csReplicating:
    conn.state = csClosed

proc startReplication*(
    conn: PgConnection,
    slotName: string,
    startLsn: Lsn = InvalidLsn,
    options: seq[(string, string)] = @[],
    autoKeepaliveReply: bool = true,
    statusInterval: async_backend.Duration = ZeroDuration,
    callback: ReplicationCallback,
): Future[void] {.async.} =
  ## Begin logical replication streaming from the given slot.
  ##
  ## The ``callback`` is invoked for each ``XLogData`` or ``PrimaryKeepalive``
  ## message received. The callback is awaited, providing natural TCP backpressure.
  ## From the callback, acknowledge durable progress with ``confirmFlushed`` (the
  ## default path; see below). With ``autoKeepaliveReply = false`` you instead
  ## drive replies yourself with ``sendStandbyStatus``; do not mix the two, since
  ## the auto-reply would report a flush position behind your manual ACKs.
  ##
  ## When ``autoKeepaliveReply`` is true (the default), the library responds
  ## automatically to ``PrimaryKeepalive`` messages with ``replyRequested = true``
  ## *before* invoking the callback. The reply reports the highest
  ## ``receivedEndLsn`` (``startLsn + data.len``) observed so far across received
  ## ``XLogData`` messages — or the caller-supplied ``startLsn`` if no
  ## ``XLogData`` has arrived yet — as the **receive** LSN, which resets
  ## ``wal_sender_timeout`` and prevents silent disconnects when the callback is
  ## slow. The **flush/apply** LSN, however, carries only what you have confirmed
  ## durable via ``confirmFlushed`` (initially ``startLsn``), *not* the receive
  ## LSN. This keeps ``confirmed_flush_lsn`` from advancing past WAL the callback
  ## has not yet persisted, so a crash re-streams unprocessed changes
  ## (at-least-once delivery). The keepalive is still delivered to the callback.
  ##
  ## To advance the slot, call ``confirmFlushed(conn, lsn)`` from the callback
  ## once the received changes are durable. The confirmed position reaches the
  ## server on the next reply-requested keepalive and on ``stopReplication`` (a
  ## clean stop flushes it), not on the ``confirmFlushed`` call itself. Set
  ## ``autoKeepaliveReply = false`` to manage replies entirely by hand with
  ## ``sendStandbyStatus`` instead — for example, to batch acknowledgements or
  ## report apply separately from flush.
  ##
  ## Until ``confirmFlushed`` is called and while ``startLsn`` is at its default
  ## ``InvalidLsn`` (``0/0``), the auto-reply carries ``0/0`` for flush/apply.
  ## PostgreSQL treats this as "position unknown" and will not move
  ## ``confirmed_flush_lsn`` backwards, so the reply is still useful for resetting
  ## ``wal_sender_timeout`` without risking data loss.
  ##
  ## **Synchronous standbys:** because the auto-reply reports receive and
  ## flush/apply separately, a consumer listed in ``synchronous_standby_names``
  ## (with ``synchronous_commit`` at ``on``/``remote_write``/``remote_apply``)
  ## that never calls ``confirmFlushed`` keeps ``wal_sender_timeout`` reset via
  ## the receive field yet never advances flush — so the primary's ``COMMIT``s
  ## block indefinitely waiting for a flush confirmation that never arrives.
  ## Call ``confirmFlushed`` promptly (or manage replies manually) in that setup.
  ##
  ## **Proactive status interval:** ``statusInterval`` (``ZeroDuration`` = off,
  ## the default) makes the library send a Standby Status Update on its own at
  ## least that often, in addition to answering reply-requested keepalives. The
  ## proactive update reports the highest received LSN as receive and the
  ## ``confirmFlushed`` position as flush/apply — same as the auto-reply — so it
  ## advances ``confirmed_flush_lsn`` (letting the server recycle WAL) without
  ## ever flushing past unconfirmed WAL. Set it when the server uses
  ## ``wal_sender_timeout = 0`` (or a long timeout): such a server never asks for
  ## a reply, so without a proactive interval the slot only advances on
  ## ``stopReplication`` and WAL accumulates meanwhile. It is honoured only when
  ## ``autoKeepaliveReply`` is true; under manual reply management drive the
  ## cadence yourself with ``sendStandbyStatus``. Under **asyncdispatch** the
  ## interval only fires while messages are flowing (it cannot safely interrupt a
  ## blocked read, so a fully idle stream sends nothing until the next message);
  ## **chronos** honours it even on a completely idle stream.
  ##
  ## If the auto-reply itself fails (for example, the connection is lost
  ## between receiving the keepalive and writing the Standby Status Update),
  ## the exception is propagated out of ``startReplication`` and the callback
  ## is *not* invoked for that keepalive.
  ##
  ## **Errors invalidate the connection.** If the ``callback`` raises — or the
  ## stream fails for any other reason mid-flight — the exception propagates out
  ## of this proc and the connection is poisoned (marked closed): the CopyBoth
  ## exchange is left half-open and the protocol stream is out of sync, so the
  ## connection cannot be reused. There is no built-in reconnect; treat the
  ## connection as dead, ``close`` it (a pool discards it automatically), and
  ## resume on a fresh connection. Because ``confirmFlushed`` /
  ## ``confirmedFlushLsn`` reset once the stream ends, track the last LSN you
  ## confirmed durable yourself in the callback so you know the restart point,
  ## then pass it as ``startLsn`` on the new stream. See
  ## ``examples/replication.nim`` for a reconnect-and-resume loop.
  ##
  ## The proc returns when the server sends ``CopyDone`` or the connection closes.
  ## To stop replication from the client side, call ``stopReplication`` from within
  ## the callback (or from a concurrent task).
  ##
  ## The bundled ``pgoutput`` decoder (``parsePgOutputMessage`` /
  ## ``decodePgOutput``) supports protocol version 1 only. Passing a
  ## ``proto_version`` other than ``1`` in ``options`` raises ``ValueError``,
  ## because a v2/v3 stream reshapes and adds messages the decoder cannot parse.
  # The bundled pgoutput decoder implements protocol version 1 only. A v2/v3
  # stream adds messages and reshapes existing ones (e.g. an xid prefix on
  # streamed tuples), which parsePgOutputMessage cannot decode. Reject an
  # unsupported proto_version before touching connection state so the failure is
  # a plain input error rather than a later mid-stream decode break.
  for (k, v) in options:
    if k.cmpIgnoreCase("proto_version") == 0:
      let pv = v.strip(chars = {'\'', '"', ' ', '\t'})
      if pv.len > 0 and pv != "1":
        raise newException(
          ValueError,
          "Unsupported pgoutput proto_version " & v &
            ": the bundled decoder supports proto_version 1 only",
        )

  conn.checkReady()
  conn.state = csBusy

  # Build START_REPLICATION command
  var sql =
    "START_REPLICATION SLOT " & quoteIdentifier(slotName) & " LOGICAL " & $startLsn
  if options.len > 0:
    sql.add(" (")
    for i, (k, v) in options:
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

  await conn.sendMsg(encodeQuery(sql))

  # Wait for CopyBothResponse. The block exits only via the CopyBothResponse
  # break or by raising — fillRecvBuf re-raises connection errors, and a
  # ReadyForQuery before CopyBothResponse always raises.
  var queryError: ref PgQueryError

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
            PgConnectionError, "START_REPLICATION ended without CopyBothResponse"
          )
        else:
          discard
      await conn.fillRecvBuf()

  # Track the highest end LSN of WAL data actually received from the wire —
  # computed as XLogData.startLsn + data.len, *not* XLogData.walEnd. ``walEnd``
  # is the server's current WAL position at the time the message was sent and can
  # be ahead of the bytes this message carries; acknowledging ``walEnd`` would
  # falsely advance ``confirmed_flush_lsn`` past unprocessed WAL and cause data
  # loss on slot restart. The position lives on the connection (the single source
  # of truth, read by confirmFlushed and the auto-reply); reset it and the
  # confirmed-flush position to the resume point so a reused connection does not
  # inherit a stale value.
  conn.resetReplLsnTracking(startLsn)

  # If anything unwinds the streaming loop while the connection is still
  # mid-CopyBoth — most often the user callback raising — the protocol stream is
  # left out of sync. Poison the connection on the way out so it is not stranded
  # in csReplicating (where every later call raises a misleading PgStateError).
  # The clean stop (CopyDone -> ReadyForQuery) returns it to csReady first, so
  # this is a no-op on the normal exit. See invalidateAbandonedStream.
  defer:
    conn.invalidateAbandonedStream()

  var lastStatusSent = Moment.now()
  var pendingRead: Future[void] = nil # in-flight timed read, threaded across waits
  when hasChronos:
    # If an error unwinds the loop while a timed read is still in flight, abandon
    # it so it is not left dangling on the torn-down connection. Normal exits
    # (CopyDone / ReadyForQuery) only happen after a read has been consumed, so
    # `pendingRead` is nil then and this is a no-op — never a cancel-then-reread.
    defer:
      if pendingRead != nil and not pendingRead.finished:
        pendingRead.cancelSoon()

  # Streaming loop
  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyData:
          lastStatusSent = await conn.handleReplicationData(
            msg.copyData, autoKeepaliveReply, callback, lastStatusSent
          )
        of bmkCopyDone:
          # Server ended the stream; reply with CopyDone before draining
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
      # Send a proactive status update if one is due after draining this batch
      # (covers a busy stream on both backends).
      lastStatusSent = await conn.maybeSendPeriodicStatus(
        autoKeepaliveReply, statusInterval, lastStatusSent
      )
      if conn.state == csClosed:
        raise newException(PgConnectionError, "Connection closed during replication")
      pendingRead =
        await conn.replFillRecvBuf(statusInterval, lastStatusSent, pendingRead)
      # Send again after a timed wake with no new data (idle coverage on chronos).
      lastStatusSent = await conn.maybeSendPeriodicStatus(
        autoKeepaliveReply, statusInterval, lastStatusSent
      )

  # After CopyDone, drain to ReadyForQuery
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

proc stopReplication*(conn: PgConnection): Future[void] {.async.} =
  ## Gracefully terminate the replication stream.
  ##
  ## Before sending CopyDone, this flushes the latest ``confirmFlushed`` position
  ## to the server (receive = highest WAL received, flush/apply = confirmed) so a
  ## clean shutdown does not lose the final acknowledgement. ``confirmFlushed``
  ## only records locally; without this flush the confirmed position would reach
  ## the server only on the next ``PrimaryKeepalive(replyRequested)``, which may
  ## never arrive before stop — leaving the slot behind and re-streaming the last
  ## batch on restart. When nothing has been confirmed the flush is the stream's
  ## ``startLsn`` (``0/0`` only when ``startLsn`` was left at its default
  ## ``InvalidLsn``, which PostgreSQL reads as "position unknown" and will not
  ## move the slot backwards), so manual-ACK callers are unaffected.
  ##
  ## The server responds with CopyDone and ReadyForQuery, which are handled by
  ## the ``startReplication`` recv loop.
  ##
  ## If flushing the confirmed position fails (for example because the
  ## connection is already lost), the exception propagates and CopyDone is not
  ## sent. In that situation the server has already dropped the connection, so
  ## the missing CopyDone does not change the outcome.
  if conn.state != csReplicating:
    raise newException(
      PgConnectionError,
      "stopReplication: connection is not in replicating state (state: " & $conn.state &
        ")",
    )
  await sendConfirmedStatus(conn, Lsn(conn.replMaxReceivedLsn()))
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
  ## Begin **physical** replication streaming.
  ##
  ## ``slotName`` is optional; pass ``""`` for a slot-less stream. ``timeline``
  ## is appended as ``TIMELINE n`` when non-zero — useful when the standby is
  ## following a specific timeline and must abort if the primary advanced past
  ## it.
  ##
  ## The callback contract matches ``startReplication``: each ``XLogData`` or
  ## ``PrimaryKeepalive`` is delivered as a ``ReplicationMessage``. The raw
  ## WAL bytes inside ``XLogData.data`` are the physical WAL stream; no
  ## pgoutput decoding applies.
  ##
  ## ``autoKeepaliveReply`` behaves identically to ``startReplication``: when
  ## true, ``PrimaryKeepalive(replyRequested=true)`` is answered before the
  ## callback runs, reporting the highest observed ``receivedEndLsn`` as the
  ## receive LSN and the ``confirmFlushed`` position (initially ``startLsn``) as
  ## flush/apply. For physical replication the flush LSN governs how much WAL the
  ## primary may recycle, so call ``confirmFlushed`` only once that WAL is safely
  ## on durable storage. A physical standby listed in ``synchronous_standby_names``
  ## that relies on the auto-reply must likewise call ``confirmFlushed`` (or reply
  ## manually), or the primary's synchronous ``COMMIT``s will block waiting for a
  ## flush position that never advances.
  ##
  ## ``statusInterval`` behaves as documented on ``startReplication``: a positive
  ## value makes the standby send a proactive Standby Status Update at least that
  ## often (receive = highest received, flush/apply = ``confirmFlushed``) so the
  ## primary can recycle WAL even when it never requests a reply (e.g.
  ## ``wal_sender_timeout = 0``); it is honoured only with ``autoKeepaliveReply``,
  ## and on asyncdispatch only fires while messages are flowing.
  ##
  ## Error handling matches ``startReplication``: a callback exception or any
  ## other mid-stream failure poisons the connection (marked closed) and
  ## propagates, so reconnect and resume from the last LSN you tracked durable.
  ##
  ## On a timeline switch the server may send a final result set describing
  ## the next timeline (``RowDescription`` + ``DataRow`` + ``CommandComplete``)
  ## between ``CopyDone`` and ``ReadyForQuery``. This proc drains and discards
  ## those messages; callers that need the next-timeline information should
  ## re-issue ``IDENTIFY_SYSTEM`` after this proc returns.
  conn.checkReady()
  conn.state = csBusy

  var sql = "START_REPLICATION"
  if slotName.len > 0:
    sql.add(" SLOT " & quoteIdentifier(slotName))
  sql.add(" PHYSICAL " & $startLsn)
  if timeline > 0:
    sql.add(" TIMELINE " & $timeline)

  await conn.sendMsg(encodeQuery(sql))

  # See startReplication for the invariants of this block — it can only exit
  # via the CopyBothResponse break or by raising.
  var queryError: ref PgQueryError

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
            "START_REPLICATION PHYSICAL ended without CopyBothResponse",
          )
        else:
          discard
      await conn.fillRecvBuf()

  # Highest end LSN of WAL data actually received — see startReplication for
  # why we track ``startLsn + data.len`` rather than ``walEnd``, and why the
  # position lives on the connection. Reset it and the confirmed-flush position
  # to the resume point so a reused connection does not inherit a stale value.
  conn.resetReplLsnTracking(startLsn)

  # See startReplication: poison the connection if the streaming loop unwinds
  # while still mid-CopyBoth (e.g. the callback raised), so it is not stranded in
  # csReplicating. No-op on the clean CopyDone -> ReadyForQuery exit.
  defer:
    conn.invalidateAbandonedStream()

  var lastStatusSent = Moment.now()
  var pendingRead: Future[void] = nil # in-flight timed read, threaded across waits
  when hasChronos:
    # See startReplication: drop a still-in-flight timed read if an error unwinds
    # the loop; nil (and so a no-op) on the normal CopyDone / ReadyForQuery exits.
    defer:
      if pendingRead != nil and not pendingRead.finished:
        pendingRead.cancelSoon()

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyData:
          lastStatusSent = await conn.handleReplicationData(
            msg.copyData, autoKeepaliveReply, callback, lastStatusSent
          )
        of bmkCopyDone:
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
      # See startReplication: send a proactive status update when one is due,
      # both after draining a batch and after a timed idle wake.
      lastStatusSent = await conn.maybeSendPeriodicStatus(
        autoKeepaliveReply, statusInterval, lastStatusSent
      )
      if conn.state == csClosed:
        raise newException(
          PgConnectionError, "Connection closed during physical replication"
        )
      pendingRead =
        await conn.replFillRecvBuf(statusInterval, lastStatusSent, pendingRead)
      lastStatusSent = await conn.maybeSendPeriodicStatus(
        autoKeepaliveReply, statusInterval, lastStatusSent
      )

  # After CopyDone, drain to ReadyForQuery. Accept the optional timeline-switch
  # result set (RowDescription + DataRow + CommandComplete) the server emits
  # when the stream stopped because the standby reached the end of a timeline.
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
