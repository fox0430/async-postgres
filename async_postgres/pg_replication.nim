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
    oldTuple*: seq[TupleField]
    newTuple*: seq[TupleField]

  DeleteMessage* = object ## Row deletion.
    relationId*: int32
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
  ## Parse an LSN from ``"X/Y"`` hex string. Raises ``ValueError`` on invalid format.
  let parts = s.split('/')
  if parts.len != 2:
    raise newException(ValueError, "Invalid LSN format: " & s)
  let hi = fromHex[uint64](parts[0])
  let lo = fromHex[uint64](parts[1])
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
  let numCols = readInt16At(buf, pos)
  pos += 2
  if numCols < 0:
    raise newException(PgProtocolError, "pgoutput tuple: negative column count")
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
    let numCols = readInt16At(data, pos)
    pos += 2
    if numCols < 0:
      raise newException(PgProtocolError, "pgoutput Relation: negative column count")
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
    # byte at offset 5 is 'N' (new tuple marker)
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
      let (oldFields, nextPos) = decodeTuple(data, pos)
      msg.oldTuple = oldFields
      pos = nextPos
      inc pos # skip 'N' marker for new tuple
    elif marker != 'N':
      raise newException(PgProtocolError, "Unknown Update tuple marker: " & marker)
    let (newFields, _) = decodeTuple(data, pos)
    msg.newTuple = newFields
    PgOutputMessage(kind: pomkUpdate, update: msg)
  of 'D': # Delete
    var msg = DeleteMessage()
    msg.relationId = readInt32At(data, 1)
    var pos = 5
    # byte at offset 5 is 'K' (key) or 'O' (old tuple)
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
  Lsn(uint64(msg.startLsn) + uint64(msg.data.len))

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

proc parseReplicationMessage*(copyData: seq[byte]): ReplicationMessage =
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

proc sendStandbyStatus*(
    conn: PgConnection,
    receiveLsn: Lsn,
    flushLsn: Lsn = InvalidLsn,
    applyLsn: Lsn = InvalidLsn,
    replyRequested: bool = false,
): Future[void] {.async.} =
  ## Send a Standby Status Update to the server during replication streaming.
  ## Must be called while the connection is in ``csReplicating`` state.
  if conn.state != csReplicating:
    raise newException(
      PgConnectionError,
      "sendStandbyStatus: connection is not in replicating state (state: " & $conn.state &
        ")",
    )
  let flushVal = if flushLsn == InvalidLsn: receiveLsn else: flushLsn
  let applyVal = if applyLsn == InvalidLsn: receiveLsn else: applyLsn
  let msg = encodeStandbyStatusUpdate(
    receiveLsn.toInt64,
    flushVal.toInt64,
    applyVal.toInt64,
    currentPgTimestamp(),
    if replyRequested: 1'u8 else: 0'u8,
  )
  await conn.sendMsg(msg)

proc startReplication*(
    conn: PgConnection,
    slotName: string,
    startLsn: Lsn = InvalidLsn,
    options: seq[(string, string)] = @[],
    autoKeepaliveReply: bool = true,
    callback: ReplicationCallback,
): Future[void] {.async.} =
  ## Begin logical replication streaming from the given slot.
  ##
  ## The ``callback`` is invoked for each ``XLogData`` or ``PrimaryKeepalive``
  ## message received. The callback is awaited, providing natural TCP backpressure.
  ## Within the callback, use ``sendStandbyStatus`` to acknowledge received data.
  ##
  ## When ``autoKeepaliveReply`` is true (the default), the library responds
  ## automatically to ``PrimaryKeepalive`` messages with ``replyRequested = true``
  ## *before* invoking the callback, sending the highest ``receivedEndLsn``
  ## (``startLsn + data.len``) observed so far across received ``XLogData``
  ## messages — or the caller-supplied ``startLsn`` if no ``XLogData`` has
  ## arrived yet — as receive/flush/apply LSN. This prevents silent disconnects
  ## from ``wal_sender_timeout`` when the callback is slow. The keepalive is
  ## still delivered to the callback. Set ``autoKeepaliveReply = false`` to
  ## manage replies manually — for example, when the flush/apply LSN must
  ## reflect callback-side progress (durable writes) rather than what has merely
  ## been received from the wire.
  ##
  ## If no ``XLogData`` has arrived and ``startLsn`` was left at its default
  ## ``InvalidLsn`` (``0/0``), the auto-reply will carry ``0/0`` for
  ## receive/flush/apply. PostgreSQL treats this as "position unknown" and will
  ## not move ``confirmed_flush_lsn`` backwards, so the reply is still useful
  ## for resetting ``wal_sender_timeout`` without risking data loss.
  ##
  ## If the auto-reply itself fails (for example, the connection is lost
  ## between receiving the keepalive and writing the Standby Status Update),
  ## the exception is propagated out of ``startReplication`` and the callback
  ## is *not* invoked for that keepalive.
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

  # Highest end LSN of WAL data actually received from the wire — computed as
  # XLogData.startLsn + data.len, *not* XLogData.walEnd. ``walEnd`` is the
  # server's current WAL position at the time the message was sent and can be
  # ahead of the bytes this message carries; acknowledging ``walEnd`` would
  # falsely advance ``confirmed_flush_lsn`` past unprocessed WAL and cause data
  # loss on slot restart.
  var lastReceivedLsn: Lsn = startLsn

  # Streaming loop
  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyData:
          let replMsg = parseReplicationMessage(msg.copyData)
          case replMsg.kind
          of rmkXLogData:
            let received = replMsg.xlogData.receivedEndLsn
            if received > lastReceivedLsn:
              lastReceivedLsn = received
          of rmkPrimaryKeepalive:
            if autoKeepaliveReply and replMsg.keepalive.replyRequested:
              await sendStandbyStatus(conn, lastReceivedLsn)
          await callback(replMsg)
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
      if conn.state == csClosed:
        raise newException(PgConnectionError, "Connection closed during replication")
      await conn.fillRecvBuf()

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
  ## Send CopyDone to gracefully terminate the replication stream.
  ## The server will respond with CopyDone and ReadyForQuery, which
  ## will be handled by the ``startReplication`` recv loop.
  if conn.state != csReplicating:
    raise newException(
      PgConnectionError,
      "stopReplication: connection is not in replicating state (state: " & $conn.state &
        ")",
    )
  await conn.sendMsg(@copyDoneMsg)

proc startPhysicalReplication*(
    conn: PgConnection,
    startLsn: Lsn,
    slotName: string = "",
    timeline: int32 = 0,
    autoKeepaliveReply: bool = true,
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
  ## true, ``PrimaryKeepalive(replyRequested=true)`` is acknowledged with the
  ## highest observed ``receivedEndLsn`` before the callback is invoked.
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
  # why we track ``startLsn + data.len`` rather than ``walEnd``.
  var lastReceivedLsn: Lsn = startLsn

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyData:
          let replMsg = parseReplicationMessage(msg.copyData)
          case replMsg.kind
          of rmkXLogData:
            let received = replMsg.xlogData.receivedEndLsn
            if received > lastReceivedLsn:
              lastReceivedLsn = received
          of rmkPrimaryKeepalive:
            if autoKeepaliveReply and replMsg.keepalive.replyRequested:
              await sendStandbyStatus(conn, lastReceivedLsn)
          await callback(replMsg)
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
      if conn.state == csClosed:
        raise newException(
          PgConnectionError, "Connection closed during physical replication"
        )
      await conn.fillRecvBuf()

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
