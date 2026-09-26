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
##       options = {"proto_version": "1", "publication_names": "my_pub"},
##       callback = myCallback)

import std/[deques, strutils, tables, times, options]

import async_backend, pg_protocol, pg_connection, pg_types
from pg_types/core import isPgUIntText, pgParseHexUInt32, pgParseIntView, pipOk
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
    startLsn*: Lsn
      ## Start LSN of the WAL data in this message. On a logical stream it is
      ## the decoded record's LSN, or ``InvalidLsn`` for a write that is not the
      ## last for its change (e.g. pgoutput Relation and Type messages).
    walEnd*: Lsn
      ## Informational. On a physical stream it is the end of WAL the server
      ## can send, which may be ahead of this message's data: acknowledge
      ## ``receivedEndLsn`` (``startLsn + data.len``), never ``walEnd``. On a
      ## logical stream it equals ``startLsn``; confirm ``CommitMessage.endLsn``
      ## or ``PrimaryKeepalive.walEnd`` instead. With any output plugin, the
      ## ``startLsn`` of a transaction's commit message is its end LSN.
    sendTime*: int64 ## Server send time (microseconds since PG epoch)
    data*: seq[byte] ## Raw WAL data (plugin-dependent format)

  PrimaryKeepalive* = object ## Keepalive message from the server.
    walEnd*: Lsn
      ## The walsender's sent position. On a logical stream it counts as
      ## received, so once every earlier message is processed it may be passed
      ## to ``confirmFlushed`` (``startReplication(autoConfirm = true)`` does
      ## this outside a transaction). The auto-reply to a keepalive with
      ## ``replyRequested`` is sent before the callback runs, so that
      ## confirmation goes out with the next status update (the next requested
      ## reply, ``statusInterval``, or ``stopReplication``).
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
    outputPlugin*: string ## Output plugin (only available at CREATE time)
    slotType*: string
      ## Slot type as reported by READ_REPLICATION_SLOT ("physical").
      ## Empty for CREATE_REPLICATION_SLOT results, which do not return it.
    restartTli*: int64
      ## Timeline ID associated with restart_lsn.
      ## Only populated by READ_REPLICATION_SLOT; 0 when NULL or not applicable.

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
    raise newException(PgTypeError, "Invalid LSN format (len=" & $s.len & ")")
  let context = "Invalid LSN format (len=" & $s.len & ")"
  let hi = pgParseHexUInt32(parts[0], context)
  let lo = pgParseHexUInt32(parts[1], context)
  Lsn((uint64(hi) shl 32) or uint64(lo))

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

const CommitEndLsnPos = 10
  ## pgoutput Commit: 'C', flags (1), commit LSN (8), then end LSN (8).

proc commitEndLsn(data: openArray[byte]): Lsn {.inline.} =
  Lsn(cast[uint64](readInt64At(data, CommitEndLsnPos)))

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
    msg.endLsn = commitEndLsn(data)
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
  ## (``startLsn + len(data)``). On a physical stream, use this when
  ## acknowledging received data via ``sendStandbyStatus``; do not use
  ## ``walEnd``, which may point past data this message does not carry.
  ##
  ## Physical replication only: logical ``data`` is plugin output, not WAL
  ## bytes, so this may point past commits not yet sent. Confirm logical
  ## progress with ``CommitMessage.endLsn`` or ``PrimaryKeepalive.walEnd``.
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

proc parseTimelineIdText(s: string, what: string): int32 =
  ## Parse unsigned-decimal timeline id with int32-range check (`what` for errors).
  if not isPgUIntText(s):
    raise newException(
      PgTypeError, what & " returned a non-numeric timeline (len=" & $s.len & ")"
    )
  var t: int
  if pgParseIntView(s, t) != pipOk:
    raise newException(
      PgTypeError, what & " returned a non-numeric timeline (len=" & $s.len & ")"
    )
  if t < int(int32.low) or t > int(int32.high):
    raise newException(
      PgTypeError, what & " returned a timeline out of int32 range (len=" & $s.len & ")"
    )
  t.int32

proc parseTimelineId*(s: string): int32 =
  ## Parse timeline id from an ``IDENTIFY_SYSTEM`` row (text format).
  ## Non-numeric/out-of-range values raise `PgTypeError`, not `RangeDefect`.
  parseTimelineIdText(s, "IDENTIFY_SYSTEM")

# Replication commands (via simple query protocol)

proc identifySystem*(
    conn: PgConnection, timeout: async_backend.Duration = ZeroDuration
): Future[SystemInfo] {.async.} =
  ## Execute ``IDENTIFY_SYSTEM`` and return system identification info.
  ##
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
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

proc quoteReplLiteral(s: string): string =
  ## Single-quote a walsender option value. Unlike `quoteLiteral` this never
  ## emits the ``E'...'`` form: the replication scanner has no such rule and
  ## treats ``\`` literally, so doubling ``'`` is the whole escape. The caller
  ## rejects a NUL byte first: the wire protocol ends the query string there.
  "'" & s.replace("'", "''") & "'"

proc createReplicationSlot*(
    conn: PgConnection,
    slotName: string,
    plugin: string = "pgoutput",
    temporary: bool = false,
    timeout: async_backend.Duration = ZeroDuration,
): Future[ReplicationSlotInfo] {.async.} =
  ## Create a logical replication slot. Returns slot info including the consistent point LSN.
  ##
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
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
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
  var sql = "DROP_REPLICATION_SLOT " & quoteIdentifier(slotName)
  if wait:
    sql.add(" WAIT")
  discard await conn.simpleQuery(sql, timeout)

proc decodeReadSlotRow(qr: QueryResult, slotName: string): ReplicationSlotInfo =
  ## Decode one READ_REPLICATION_SLOT result row.
  ##
  ## The server returns exactly 3 columns: slot_type (text, "physical" or NULL
  ## when the slot does not exist), restart_lsn (text LSN or NULL when never
  ## reserved), restart_tli (int8 as text or NULL). Only physical slots are
  ## supported server-side; a logical slot raises PgQueryError before any row
  ## is returned.
  if qr.fields.len < 3:
    raise newException(
      PgConnectionError,
      "READ_REPLICATION_SLOT returned " & $qr.fields.len & " columns, expected >= 3",
    )
  let row = initRow(qr.data, 0)
  # A nonexistent slot yields one row with all NULLs, not zero rows.
  if row.isNull(0):
    raise newException(
      PgConnectionError,
      "READ_REPLICATION_SLOT: replication slot \"" & slotName & "\" does not exist",
    )
  result.slotName = slotName
  result.slotType = row.getStr(0)
  if not row.isNull(1):
    result.consistentPoint = parseLsn(row.getStr(1))
  if not row.isNull(2):
    # Same range check as IDENTIFY_SYSTEM.
    result.restartTli =
      parseTimelineIdText(row.getStr(2), "READ_REPLICATION_SLOT").int64

proc readReplicationSlot*(
    conn: PgConnection, slotName: string, timeout: async_backend.Duration = ZeroDuration
): Future[ReplicationSlotInfo] {.async.} =
  ## Read information about an existing physical replication slot.
  ##
  ## Only physical slots are supported: the server rejects a logical slot with
  ## ``PgQueryError`` ("cannot use READ_REPLICATION_SLOT with logical
  ## replication slot") and reports a nonexistent slot as ``PgConnectionError``.
  ## ``consistentPoint`` carries restart_lsn (``InvalidLsn`` when the slot never
  ## reserved WAL) and ``restartTli`` its timeline (0 when NULL).
  ##
  ## On timeout, the connection is retired (csClosed) unless the wire had
  ## settled (asyncdispatch always retires: the timed-out op stays on the socket).
  let results = await conn.simpleQuery(
    "READ_REPLICATION_SLOT " & quoteIdentifier(slotName), timeout
  )
  if results.len == 0 or results[0].rowCount == 0:
    raise newException(PgConnectionError, "READ_REPLICATION_SLOT returned no results")
  return decodeReadSlotRow(results[0], slotName)

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

proc replClosedError(conn: PgConnection, msg = "Connection is closed"): ref PgError =
  ## ``raiseClosedConnection``'s error, or nil while open, keeping the
  ## replication write that killed the connection as the cause.
  case conn.closedReason
  of crOpen:
    nil
  of crClosedByUser:
    (ref PgStateError)(
      msg: closedByUserMsg,
      parent: newException(PgConnectionError, msg, conn.replWriteFailure),
    )
  of crClosed:
    conn.newClosedError(msg, conn.replWriteFailure)

proc checkReplicating(conn: PgConnection, op: string) =
  ## ``csReplicating`` guard for a mid-stream operation. A connection the
  ## application closed itself, or one simply never put into a stream, is a
  ## caller-state error (``PgStateError``); only a lost transport is a
  ## ``PgConnectionError``, so a reconnect loop does not fire on a shutdown the
  ## application requested.
  # ``closedByUser`` first, as ``checkReady`` does: ``close()`` sets it while the
  # connection is still ``csReplicating``.
  let closed = conn.replClosedError()
  if closed != nil:
    raise closed
  if conn.state == csReplicating:
    return
  raise newException(
    PgStateError,
    op & ": connection is not in replicating state (state: " & $conn.state & ")",
  )

const StandbyStatusLen = 1 + 8 + 8 + 8 + 8 + 1
  ## 'r' + receive + flush + apply + clock + replyRequested.

# Replication write queue. Every CopyData and CopyDone the client writes during
# a stream is queued without suspending and written in queue order by one task.
# Whether the client's CopyDone is out and which positions a caller reported are
# settled when queued, so concurrent callers cannot interleave or undercut each
# other. Cancelling a caller only ends its wait; its frame still goes out whole.

proc replWriteError(conn: PgConnection): ref CatchableError =
  ## A fresh error per waiter of a write that did not make it: one raised
  ## exception cannot be shared between futures.
  result = conn.replClosedError()
  if result == nil:
    result = newException(
      PgStateError, "the replication stream ended before this write",
      conn.replWriteFailure,
    )

proc settle(conn: PgConnection, w: ReplWrite, ok: bool) =
  w.state = if ok: rwWritten else: rwFailed
  for fut in w.waiters:
    if fut.finished: # the caller cancelled its wait
      continue
    if ok:
      fut.complete()
    else:
      fut.fail(conn.replWriteError())
  w.waiters.setLen(0)

proc failQueuedReplWrites(conn: PgConnection) =
  ## Fail every write not yet started. The one being written settles itself.
  conn.replPendingStatus = nil
  while conn.replWrites.len > 0:
    conn.settle(conn.replWrites.popFirst(), ok = false)
  for w in [conn.replFinalStatus, conn.replCopyDone]:
    if w != nil and w.state == rwQueued:
      conn.settle(w, ok = false)

proc closeReplWrites(conn: PgConnection) =
  ## The stream is ending: accept no more writes and fail those not started.
  conn.replWritesOpen = false
  conn.failQueuedReplWrites()

proc nextReplWrite(conn: PgConnection): ReplWrite =
  ## The queue in order, then the stop's final status and CopyDone, so nothing
  ## queued before the final status is encoded can land after CopyDone.
  if conn.replWrites.len > 0:
    return conn.replWrites.popFirst()
  for w in [conn.replFinalStatus, conn.replCopyDone]:
    if w != nil and w.state == rwQueued:
      if w == conn.replFinalStatus and conn.replInCallback:
        # The running callback may still confirm a position for it.
        return nil
      return w

proc encodeStatus(
    receiveLsn, flushLsn, applyLsn: Lsn, replyRequested: bool
): seq[byte] =
  ## Standby Status Update with the given LSNs verbatim — no ``InvalidLsn``
  ## defaulting. The single place the wire encoding lives.
  encodeStandbyStatusUpdate(
    receiveLsn.toInt64,
    flushLsn.toInt64,
    applyLsn.toInt64,
    currentPgTimestamp(),
    if replyRequested: 1'u8 else: 0'u8,
  )

proc encodeConfirmedStatus(conn: PgConnection): tuple[msg: seq[byte], flush: uint64] =
  ## The library's Standby Status Update, encoded as it is written: highest
  ## received position in *receive* (resets ``wal_sender_timeout``), the
  ## ``confirmFlushed`` position in flush/apply. Unconfirmed with the default
  ## ``startLsn`` that is ``0/0``, which PostgreSQL treats as "unknown".
  ##
  ## Each field is raised to the caller's last reported status: a lower flush
  ## would move a physical slot's ``restart_lsn`` backwards.
  let confirmed = conn.replConfirmedFlushLsn()
  let reported = conn.replReported
  let flushLsn = max(confirmed, reported.flush)
  let applyLsn = max(confirmed, reported.apply)
  let receive =
    max(max(conn.replMaxReceivedLsn(), reported.receive), max(flushLsn, applyLsn))
  (encodeStatus(Lsn(receive), Lsn(flushLsn), Lsn(applyLsn), false), flushLsn)

proc flushReplWrites(conn: PgConnection) {.async.} =
  ## Write queued entries in order until none is left. A failed write closes the
  ## connection, so it and every entry behind it fail. Never fails itself.
  var w: ReplWrite
  try:
    while true:
      w = conn.nextReplWrite()
      if w == nil:
        break
      if w == conn.replPendingStatus:
        conn.replPendingStatus = nil
      w.state = rwWriting
      if conn.state != csReplicating or conn.closedReason != crOpen:
        # Nothing was written, so the transport is left alone.
        conn.settle(w, ok = false)
        conn.failQueuedReplWrites()
        return
      if w.frame.len > 0:
        await conn.sendMsg(move(w.frame))
      else:
        # The library's status carries the positions current when it goes
        # out. Counted as sent from here: a failed write ends the stream.
        let (msg, flush) = conn.encodeConfirmedStatus()
        conn.replSentFlushRaw = flush
        await conn.sendMsg(msg)
      conn.settle(w, ok = true)
  except CatchableError as e:
    # A failed write (sendMsg has marked the connection closed, possibly
    # mid-frame) or anything unexpected: the wire can no longer be trusted.
    # Retire the connection and settle every waiter. chronos: drop the socket
    # so the recv loop ends too. asyncdispatch: closing would unregister the
    # pending read and strand the recv loop.
    conn.replWriteFailure = e
    conn.markClosed()
    when hasChronos:
      try:
        await noCancel conn.closeTransport()
      except CatchableError:
        discard
    if w != nil and w.state == rwWriting:
      conn.settle(w, ok = false)
    conn.closeReplWrites()

proc waitReplWrite(w: ReplWrite): Future[void] =
  result = newFuture[void]("replWrite")
  w.waiters.add(result)

proc startFlush(conn: PgConnection) =
  ## Drain ``replWrites`` unless a task already does. Waiters go on before
  ## this: the flush may finish an entry without suspending.
  if conn.replFlusher == nil or conn.replFlusher.finished:
    conn.replFlusher = conn.flushReplWrites()

proc tailPendingStatus(conn: PgConnection): ReplWrite =
  ## The library's status still queued at the tail, or nil. It is encoded when
  ## written, so it already carries anything newer and can stand for another.
  let pending = conn.replPendingStatus
  if pending != nil and conn.replWrites.peekLast == pending:
    return pending

proc queueConfirmedStatus(conn: PgConnection): ReplWrite =
  ## Queue the library's status, or share the one at the tail.
  result = conn.tailPendingStatus()
  if result != nil:
    return
  result = ReplWrite()
  conn.replWrites.addLast(result)
  conn.replPendingStatus = result

proc canStillReport(conn: PgConnection): bool =
  ## Whether a position recorded now still reaches the server: always before
  ## the client's stop, then only until the stop's final status is encoded,
  ## which waits for a running callback to return.
  conn.replWritesOpen and
    (conn.replCopyDone == nil or conn.replFinalStatus.state == rwQueued)

proc confirmReportable(conn: PgConnection, lsn: uint64): bool =
  ## ``confirmReplFlushed`` while the position can still reach the server.
  conn.canStillReport() and conn.confirmReplFlushed(lsn)

proc awaitReplWritesIdle(conn: PgConnection) {.async.} =
  ## Close the stream to writes and wait out the one being written, so no
  ## replication frame is still going out once the connection is handed back.
  conn.closeReplWrites()
  while (let flusher = conn.replFlusher; flusher != nil and not flusher.finished):
    # chronos: a cancel of this wait must not cancel the write mid-frame.
    when hasChronos:
      await flusher.join()
    else:
      await flusher

proc queueCallerCopyData(
    conn: PgConnection,
    op: string,
    frame: sink seq[byte],
    reported = none(tuple[receive, flush, apply: uint64]),
): Future[void] =
  ## Queue a caller's CopyData; a Standby Status Update records ``reported``
  ## now so later library statuses never go below it. Dropped rather than
  ## raised once the stop's final status is encoded: raising would end the
  ## stream for a callback acking a message from before the stop.
  if not conn.replWritesOpen:
    raise newException(PgStateError, op & ": the replication stream has ended")
  if not conn.canStillReport():
    result = newFuture[void]("replWriteDropped")
    result.complete()
    return
  if reported.isSome:
    let r = reported.get
    conn.noteReplReported(r.receive, r.flush, r.apply)
  let w = ReplWrite(frame: frame)
  result = w.waitReplWrite()
  conn.replWrites.addLast(w)
  conn.startFlush()

proc sendCopyData*(conn: PgConnection, data: openArray[byte]): Future[void] =
  ## Send CopyData during ``csReplicating``. Raises ``PgStateError`` (not
  ## replicating) / ``PgConnectionError`` (connection lost) / ``PgTypeError``
  ## synchronously before first suspension. ``data`` is encoded into the frame
  ## there too, so the caller's buffer need not outlive the returned ``Future``.
  ## A hand-built Standby Status Update is recorded like ``sendStandbyStatus``.
  ## Writes go out in call order with the library's own. After
  ## ``stopReplication`` (or the reply to a server-initiated stop) the frame
  ## still goes out ahead of the stop's final status while that is queued;
  ## once it is encoded the frame is silently dropped, as the walsender reads
  ## nothing after the client's CopyDone. Cancelling the returned ``Future``
  ## only stops the wait: the frame is still written. A failed write closes the
  ## connection.
  conn.checkReplicating("sendCopyData")
  var buf: seq[byte]
  encodeCopyData(buf, data)
  if data.len == StandbyStatusLen and data[0] == byte('r'):
    let positions: tuple[receive, flush, apply: uint64] = (
      cast[uint64](decodeInt64(data, 1)),
      cast[uint64](decodeInt64(data, 9)),
      cast[uint64](decodeInt64(data, 17)),
    )
    return conn.queueCallerCopyData("sendCopyData", buf, some(positions))
  conn.queueCallerCopyData("sendCopyData", buf)

proc sendStandbyStatus*(
    conn: PgConnection,
    receiveLsn: Lsn,
    flushLsn: Lsn = InvalidLsn,
    applyLsn: Lsn = InvalidLsn,
    replyRequested: bool = false,
): Future[void] {.async.} =
  ## Send Standby Status Update. ``InvalidLsn`` defaults up to ``receiveLsn``.
  ## Raises ``PgStateError`` unless the connection is ``csReplicating``, or
  ## ``PgConnectionError`` when the connection was lost. After
  ## ``stopReplication`` the update still goes out, ahead of the stop's final
  ## status, while that is queued; once it is encoded the update is silently
  ## dropped, as the walsender reads nothing after the client's CopyDone.
  ## Report before stopping to be sure. Cancelling only
  ## stops the wait: the update is still written. A failed write closes the
  ## connection.
  ##
  ## Values are sent verbatim, unlike ``confirmFlushed``. On a logical stream
  ## pass ``CommitMessage.endLsn`` or ``PrimaryKeepalive.walEnd``, never
  ## ``receivedEndLsn``: a flush past an unsent commit makes the server skip it.
  ## The auto-reply, periodic status and ``stopReplication`` never report less
  ## than the last update sent here: they send the ``confirmFlushed`` position
  ## or these, whichever is higher.
  conn.checkReplicating("sendStandbyStatus")
  let flushVal = if flushLsn == InvalidLsn: receiveLsn else: flushLsn
  let applyVal = if applyLsn == InvalidLsn: receiveLsn else: applyLsn
  await conn.queueCallerCopyData(
    "sendStandbyStatus",
    encodeStatus(receiveLsn, flushVal, applyVal, replyRequested),
    some(
      (receive: receiveLsn.toUInt64, flush: flushVal.toUInt64, apply: applyVal.toUInt64)
    ),
  )

proc confirmedFlushLsn*(conn: PgConnection): Lsn {.inline.} =
  ## Confirmed flush LSN for current stream, or ``InvalidLsn`` outside stream.
  # ``closedByUser`` leads, matching ``checkReplicating``: ``close()`` sets it
  # while still ``csReplicating``, and the stream is over from that point on.
  if conn.closedByUser or conn.state != csReplicating:
    return InvalidLsn
  Lsn(conn.replConfirmedFlushLsn())

proc confirmFlushed*(conn: PgConnection, lsn: Lsn): bool =
  ## Confirm WAL up to ``lsn`` as durable. Clamped to received WAL, monotonic.
  ## Returns true if advanced. Must be in ``csReplicating``. Received WAL is the
  ## highest ``receivedEndLsn`` (physical) or ``XLogData.startLsn`` /
  ## ``PrimaryKeepalive.walEnd`` (logical). Returns false without advancing once
  ## ``stopReplication``'s final status is encoded: the position could no
  ## longer be reported. That waits for a running callback to return, so a
  ## callback may still confirm after calling ``stopReplication``.
  conn.checkReplicating("confirmFlushed")
  # Clamp to received WAL: durably-persisted WAL can never exceed what was
  # received. Clamping (rather than raising) keeps automatic replies from
  # emitting flush ahead of receive without letting an out-of-range LSN — e.g.
  # the readily-available ``walEnd`` — throw out of the callback and strand the
  # connection in ``csReplicating``. The raw helper in pg_connection/types
  # performs the clamp and the monotonic advance in one place.
  return conn.confirmReportable(lsn.toUInt64)

proc sendConfirmedStatus(conn: PgConnection): Future[bool] {.async.} =
  ## Queue the library's status and wait for it. False, sending nothing, once
  ## the client's CopyDone is queued. Raises ``PgStateError`` outside an active
  ## replication stream.
  conn.checkReplicating("sendConfirmedStatus")
  if conn.replCopyDone != nil or not conn.replWritesOpen:
    return false
  let fut = conn.queueConfirmedStatus().waitReplWrite()
  conn.startFlush()
  await fut
  return true

proc stopStream(conn: PgConnection): Future[void] =
  ## Queue the client's end of the stream: a last status, then CopyDone. Once
  ## it is queued, a later stop waits on that same CopyDone and shares its
  ## outcome.
  let copyDone = conn.replCopyDone
  if copyDone == nil:
    if not conn.replWritesOpen:
      result = newFuture[void]("replStopEnded")
      result.fail(newException(PgStateError, "the replication stream has ended"))
      return
    # A library status still queued at the tail becomes the final one.
    let pending = conn.tailPendingStatus()
    if pending != nil:
      discard conn.replWrites.popLast()
      conn.replPendingStatus = nil
      conn.replFinalStatus = pending
    else:
      conn.replFinalStatus = ReplWrite()
    let copyDone = ReplWrite(frame: @copyDoneMsg)
    conn.replCopyDone = copyDone
    # A failed status fails the CopyDone behind it, which reports it.
    result = copyDone.waitReplWrite()
    conn.startFlush()
    return
  case copyDone.state
  of rwQueued, rwWriting:
    result = copyDone.waitReplWrite()
  of rwFailed:
    result = newFuture[void]("replStopFailed")
    result.fail(conn.replWriteError())
  of rwWritten:
    result = newFuture[void]("replStopDone")
    result.complete()

proc resetReplLsnTracking(
    conn: PgConnection, startLsn: Lsn, autoConfirm = false, serverFlush = InvalidLsn
) =
  ## Reset the per-stream confirmed-flush and max-received positions to the
  ## resume point at the start of a stream, so a reused connection never inherits
  ## a stale value from a previous stream. The confirmed-flush position then
  ## advances only via ``confirmFlushed``; the max-received position advances as
  ## ``XLogData`` (and, on a logical stream, ``PrimaryKeepalive``) arrives and
  ## bounds what ``confirmFlushed`` will accept.
  conn.initReplLsnTracking(startLsn.toUInt64)
  conn.replFinalStatus = nil
  conn.replWriteFailure = nil
  conn.replPendingStatus = nil
  conn.replCopyDone = nil
  conn.replWritesOpen = true
  conn.replAutoConfirm = autoConfirm
  conn.replInTxn = false
  conn.replInCallback = false
  # What the server already holds, not startLsn: autoConfirm reports anything
  # above it, a start ahead of the slot included.
  conn.replSentFlushRaw = serverFlush.toUInt64

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
  ## e.g. a server configured with ``wal_sender_timeout = 0``. The update is the
  ## automatic keepalive reply's (see ``sendConfirmedStatus``): receive = highest
  ## received, flush/apply = the ``confirmFlushed`` position, never below the
  ## caller's last reported status. Returns the timestamp to
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
  # After the client's CopyDone nothing is sent, but the clock still restarts:
  # otherwise the elapsed interval re-arms the recv loop's timer every tick.
  discard await sendConfirmedStatus(conn)
  return Moment.now()

type ReplStreamKind = enum
  rskLogical
  rskPhysical

func label(kind: ReplStreamKind): string =
  case kind
  of rskLogical: "replication"
  of rskPhysical: "physical replication"

proc handleReplicationData(
    conn: PgConnection,
    copyData: sink seq[byte],
    autoKeepaliveReply: bool,
    kind: ReplStreamKind,
    callback: ReplicationCallback,
    lastStatusSent: Moment,
): Future[Moment] {.async.} =
  ## Process one CopyData frame from a replication stream: parse it, advance the
  ## received-WAL position (the single source of truth read by
  ## ``confirmFlushed`` and the auto-reply), emit an automatic keepalive reply on
  ## a ``PrimaryKeepalive`` with ``replyRequested`` when ``autoKeepaliveReply``
  ## is set (and, under ``autoConfirm`` regardless of it, on one outside a
  ## transaction with an unreported position), then invoke the user
  ## ``callback``. Shared by ``startReplication`` and
  ## ``startPhysicalReplication`` so the received-tracking and auto-reply logic
  ## lives in exactly one place.
  ##
  ## Returns the timestamp to record as ``lastStatusSent``; it is updated when
  ## an automatic keepalive reply is sent so that ``statusInterval`` tracks the
  ## last time the server saw a Standby Status Update, preventing duplicate
  ## proactive updates.
  var newLastStatusSent = lastStatusSent
  let replMsg = parseReplicationMessage(move(copyData))
  # autoConfirm: a Commit is confirmed only once the callback has processed it.
  var commitEnd = InvalidLsn
  var sawCommit = false
  case replMsg.kind
  of rmkXLogData:
    # Logical data isn't WAL bytes; a Commit's startLsn equals its endLsn.
    let received =
      case kind
      of rskLogical: replMsg.xlogData.startLsn
      of rskPhysical: replMsg.xlogData.receivedEndLsn
    discard conn.updateReplMaxReceivedLsn(received.toUInt64)
    if conn.replAutoConfirm and replMsg.xlogData.data.len > 0:
      case char(replMsg.xlogData.data[0])
      of 'B':
        conn.replInTxn = true
      of 'C':
        # A short frame is left for the callback's own parse to reject.
        sawCommit = true
        if replMsg.xlogData.data.len >= CommitEndLsnPos + 8:
          commitEnd = commitEndLsn(replMsg.xlogData.data)
      else:
        discard
  of rmkPrimaryKeepalive:
    # walEnd is the walsender's sent position; on a logical stream every commit
    # before it was already streamed. Physical keeps the byte-exact XLogData bound.
    if kind == rskLogical:
      discard conn.updateReplMaxReceivedLsn(replMsg.keepalive.walEnd.toUInt64)
    if conn.replAutoConfirm and not conn.replInTxn:
      # Everything before it is processed; pgoutput reports skipped
      # transactions only through walEnd.
      discard conn.confirmReportable(replMsg.keepalive.walEnd.toUInt64)
    if autoKeepaliveReply and replMsg.keepalive.replyRequested:
      if await sendConfirmedStatus(conn):
        newLastStatusSent = Moment.now()
    elif conn.replAutoConfirm and not conn.replInTxn and conn.replPendingStatus == nil and
        conn.replConfirmedFlushLsn() >
        max(conn.replSentFlushRaw, conn.replReported.flush):
      # Report a confirmation the server has not heard yet. Awaited so a failed
      # write surfaces here; asyncdispatch's recv loop would miss it until more data.
      if await sendConfirmedStatus(conn):
        newLastStatusSent = Moment.now()
  conn.replInCallback = true
  try:
    await callback(replMsg)
    if sawCommit:
      conn.replInTxn = false
      discard conn.confirmReportable(commitEnd.toUInt64)
  finally:
    conn.replInCallback = false
  if conn.replFinalStatus != nil and conn.replFinalStatus.state == rwQueued and
      conn.replWritesOpen:
    # A stop held back while the callback ran goes out now.
    conn.startFlush()
  return newLastStatusSent

proc raiseIfStreamClosed(
    conn: PgConnection, kind: ReplStreamKind, queryError: ref PgQueryError = nil
) =
  ## Raise if the connection died under the stream, naming the replication
  ## write that killed it as the cause, and the server's error if it sent one.
  if conn.state != csClosed:
    return
  var msg = "Connection closed during " & kind.label
  if queryError != nil:
    msg.add(" after the server's error: " & queryError.msg)
  raise conn.replClosedError(msg)

proc finishStream(
    conn: PgConnection,
    kind: ReplStreamKind,
    txStatus: TransactionStatus,
    queryError: ref PgQueryError,
) {.async.} =
  ## ReadyForQuery ends the stream: settle our writes, then hand the connection
  ## back unless a write that failed meanwhile left it dead.
  await conn.awaitReplWritesIdle()
  conn.raiseIfStreamClosed(kind, queryError)
  conn.txStatus = txStatus
  conn.markReady()
  if queryError != nil:
    raise queryError

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
    conn.markClosed()

proc runReplicationStream(
    conn: PgConnection,
    startLsn: Lsn,
    autoKeepaliveReply: bool,
    statusInterval: async_backend.Duration,
    kind: ReplStreamKind,
    callback: ReplicationCallback,
    autoConfirm = false,
    serverFlush = InvalidLsn,
): Future[void] {.async.} =
  ## Shared replication stream body. Caller must have already sent the
  ## ``START_REPLICATION`` query. ``kind`` selects the received-LSN rule and
  ## labels error messages.
  var queryError: ref PgQueryError

  # Register the poison-on-abandon defer BEFORE waitCopyBoth so a raise during
  # the CopyBothResponse wait (state still csBusy) is also poisoned, not just a
  # mid-stream failure once csReplicating.
  defer:
    conn.invalidateAbandonedStream()
    # Only an abandoned stream leaves writes queued; nothing will flush them.
    conn.closeReplWrites()

  block waitCopyBoth:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyBothResponse:
          conn.markState(csReplicating)
          break waitCopyBoth
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          conn.txStatus = msg.txStatus
          conn.markReady()
          if queryError != nil:
            raise queryError
          raise newException(
            PgConnectionError,
            "START_REPLICATION " & kind.label & " ended without CopyBothResponse",
          )
        else:
          discard
      await conn.fillRecvBuf()

  conn.resetReplLsnTracking(startLsn, autoConfirm, serverFlush)

  var lastStatusSent = Moment.now()
  var pendingRead: Future[void] = nil
  when hasChronos:
    defer:
      if pendingRead != nil and not pendingRead.finished:
        pendingRead.cancelSoon()

  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        # A failed write ends the stream even with messages still buffered.
        conn.raiseIfStreamClosed(kind)
        var msg = opt.get
        case msg.kind
        of bmkCopyData:
          lastStatusSent = await conn.handleReplicationData(
            move(msg.copyData), autoKeepaliveReply, kind, callback, lastStatusSent
          )
        of bmkCopyDone:
          # Mirror only on server-initiated stop (walsender timeout,
          # pg_terminate_backend, slot drop). If the client already sent
          # CopyDone via stopReplication, the protocol allows no second one
          # (PostgreSQL ignores it once out of COPY mode).
          await conn.stopStream()
          break recvLoop
        of bmkErrorResponse:
          queryError = newPgQueryError(msg.errorFields)
        of bmkReadyForQuery:
          # The server left COPY on an error: settle our writes before the
          # connection is handed back.
          await conn.finishStream(kind, msg.txStatus, queryError)
          return
        else:
          discard
      lastStatusSent = await conn.maybeSendPeriodicStatus(
        autoKeepaliveReply, statusInterval, lastStatusSent
      )
      conn.raiseIfStreamClosed(kind)
      # Without autoKeepaliveReply, lastStatusSent never advances, so a timer
      # race here would rearm every ~1 ms.
      let effectiveInterval = if autoKeepaliveReply: statusInterval else: ZeroDuration
      try:
        pendingRead =
          await conn.replFillRecvBuf(effectiveInterval, lastStatusSent, pendingRead)
      except PgConnectionError as e:
        # chronos: a failed write drops the socket, so the read fails next;
        # name the write as the cause.
        if e.parent == nil:
          e.parent = conn.replWriteFailure
        raise e
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
          await conn.finishStream(kind, msg.txStatus, queryError)
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
    autoConfirm: bool = false,
): Future[void] {.async.} =
  ## Begin logical replication. Callback invoked per message. Use
  ## ``confirmFlushed`` for flush tracking; or set ``autoKeepaliveReply=false``
  ## and use ``sendStandbyStatus`` manually.
  ##
  ## ``autoConfirm`` (pgoutput only) confirms progress for you: a transaction's
  ## ``CommitMessage.endLsn`` once the callback has returned for its Commit, and
  ## a keepalive's ``walEnd`` outside a transaction (reported right away), so
  ## the slot also advances while pgoutput skips unpublished transactions. The
  ## callback returning therefore means "processed durably"; a callback raising
  ## on a transaction's messages confirms nothing of it. A keepalive carries no
  ## data, so its position is confirmed before its callback runs.
  ##
  ## Nothing is confirmed once ``stopReplication``'s final status is encoded.
  ## That waits for a running callback to return, so a stop from a Commit's
  ## own callback still reports that transaction; messages after it are not
  ## confirmed.
  ##
  ## To resume after an error, reconnect and pass ``InvalidLsn``: the server
  ## restarts from the slot's ``confirmed_flush_lsn``, re-sending
  ## anything not yet reported. The slot's ``confirmed_flush_lsn`` is looked up
  ## first and the stream starts from it or ``startLsn``, whichever is later
  ## (the server would not start earlier either), so keepalives seen while the
  ## server re-reads WAL from ``restart_lsn`` never report a position below it.
  ## A Commit's position is confirmed at once but reaches the server with the
  ## next status: a keepalive's, ``statusInterval``'s or the stop's. Set
  ## ``statusInterval`` on a stream that may stay busy for long, as its
  ## walsender sends no idle keepalive meanwhile.
  ##
  ## Requires ``publication_names`` in ``options`` and ``autoKeepaliveReply``
  ## (``ValueError`` otherwise).
  ##
  ## Returns on server ``CopyDone`` or connection close. To stop from the client
  ## side, call ``stopReplication`` from the callback (or a concurrent task).
  ##
  ## Errors poison connection. Track LSN for resume. A failing auto-reply
  ## propagates too, and the callback is *not* invoked for that keepalive.
  ## Option values are passed unquoted and single-quoted when building the
  ## command (keys stay identifier-validated). An empty value means a
  ## flag-only option (``binary`` rather than ``binary ''``). Raises
  ## ``PgConnectionError`` (closed) / ``PgStateError`` (busy) unless
  ## ``csReady``, and ``ValueError`` for a ``proto_version`` other than ``1``
  ## in ``options`` (the value must be the unquoted string ``"1"``, an empty
  ## one included): the bundled pgoutput decoder supports v1 only. Any value already wrapped in
  ## quotes raises ``ValueError`` too, whatever its key, so the verbatim-options
  ## spelling cannot silently name a publication ``'my_pub'`` or send a
  ## thrice-quoted ``binary`` flag the plugin rejects mid-stream. An empty
  ## ``publication_names`` and a value containing a NUL byte are rejected the
  ## same way.
  ## ``publication_names`` without an explicit ``proto_version`` adds
  ## ``proto_version '1'`` to the generated command, so a server-side default
  ## bump cannot outrun that decoder.
  ##
  ## ``statusInterval`` (``ZeroDuration`` = off) sends a proactive Standby Status
  ## Update at least that often — receive = highest received, flush/apply =
  ## ``confirmFlushed`` (never below a position the caller itself reported via
  ## ``sendStandbyStatus``) — so the slot advances on a server that never requests a
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
    if k.len == 0:
      raise newException(ValueError, "Empty replication option key")
    # Checked here, before the autoConfirm slot lookup touches the wire.
    if k[0] notin IdentStartChars or not k.allCharsInSet(IdentChars):
      raise newException(ValueError, "Invalid replication option key: " & k)
    if '\0' in v:
      raise newException(ValueError, "Replication option value contains a NUL byte")
    # Values are quoted below, so one that already arrives wrapped in quotes
    # would reach the server including them. Reject the pre-quoting spelling
    # rather than sending a value the plugin rejects mid-stream.
    if v.len >= 2 and v[0] in {'\'', '"'} and v[^1] == v[0]:
      raise newException(
        ValueError,
        "Quoted value " & v & " for replication option " & k &
          ": the value would be quoted again and reach the server including" &
          " the quotes (pass option values unquoted, e.g. \"my_pub\")",
      )
    if k.cmpIgnoreCase("proto_version") == 0:
      hasProtoVersion = true
      if v.len == 0:
        raise newException(
          ValueError,
          "Empty proto_version: an empty value is sent as a flag-only option," &
            " which pgoutput rejects (pass the unquoted value \"1\")",
        )
      if v != "1":
        raise newException(
          ValueError,
          "Unsupported pgoutput proto_version " & v &
            ": the bundled decoder supports proto_version 1 only" &
            " (pass option values unquoted, e.g. \"1\")",
        )
    elif k.cmpIgnoreCase("publication_names") == 0:
      hasPublicationNames = true
      if v.len == 0:
        raise newException(
          ValueError,
          "Empty publication_names: an empty value is sent as a flag-only" &
            " option, which pgoutput rejects (pass one or more publication" &
            " names, e.g. \"my_pub\")",
        )

  if autoConfirm:
    if not hasPublicationNames:
      raise newException(
        ValueError,
        "autoConfirm requires pgoutput (publication_names in options): it" &
          " tracks transactions through pgoutput Begin/Commit messages",
      )
    if not autoKeepaliveReply:
      raise newException(
        ValueError,
        "autoConfirm requires autoKeepaliveReply: under manual replies the" &
          " caller reports progress via sendStandbyStatus",
      )

  conn.checkReady()

  var effectiveStart = startLsn
  var slotFlush = InvalidLsn
  if autoConfirm:
    # A logical walsender starts no earlier than the slot's confirmed_flush_lsn
    # but re-reads WAL from restart_lsn, so its keepalive walEnd can trail that
    # position until it catches up. Track from the later of the two.
    let results = await conn.simpleQuery(
      "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name = " &
        quoteLiteral(slotName)
    )
    if results.len > 0 and results[0].rowCount > 0:
      if results[0].fields.len < 1:
        raise
          newException(PgConnectionError, "replication slot lookup returned no columns")
      let row = initRow(results[0].data, 0)
      if not row.isNull(0):
        slotFlush = parseLsn(row.getStr(0))
        effectiveStart = max(startLsn, slotFlush)
    # The lookup left the connection ready across a suspension; another task
    # may have taken it since.
    conn.checkReady()

  # publication_names => pgoutput; pin proto_version defensively against a
  # future server-side default bump past 1.
  var effectiveOptions = options
  if hasPublicationNames and not hasProtoVersion:
    effectiveOptions.add(("proto_version", "1"))

  # Build START_REPLICATION command. Values are single-quoted so untrusted
  # input cannot break out of the option list via the simple-query protocol.
  var sql =
    "START_REPLICATION SLOT " & quoteIdentifier(slotName) & " LOGICAL " & $effectiveStart
  if effectiveOptions.len > 0:
    sql.add(" (")
    for i, (k, v) in effectiveOptions:
      if i > 0:
        sql.add(", ")
      sql.add(k)
      if v.len > 0:
        sql.add(" " & quoteReplLiteral(v))
    sql.add(")")

  let msg = encodeQuery(sql)
  conn.markBusy()
  await conn.sendMsg(msg)
  await runReplicationStream(
    conn, effectiveStart, autoKeepaliveReply, statusInterval, rskLogical, callback,
    autoConfirm, slotFlush,
  )

proc stopReplication*(conn: PgConnection): Future[void] {.async.} =
  ## Terminate replication. Flushes confirmed position before CopyDone, never
  ## below the caller's last reported status (``sendStandbyStatus``).
  ## Raises ``PgStateError`` unless the connection is ``csReplicating``, or
  ## ``PgConnectionError`` when the connection was lost. The final status and
  ## CopyDone are queued behind every earlier replication write; a call made
  ## once they are queued waits for that same CopyDone. Cancelling only stops
  ## the wait: the stop still goes out. A failed write closes the connection.
  ##
  ## While the stream's callback runs, the final status waits for it to return,
  ## so a position it confirms after this call (``autoConfirm``'s Commit
  ## included) is still reported. A call made meanwhile, from the callback or
  ## elsewhere, returns once the stop is queued; ``startReplication`` reports
  ## a failed write. To stop from another task, await ``startReplication``
  ## rather than calling ``close()`` right after, or the stop may never go out.
  conn.checkReplicating("stopReplication")
  let stopped = conn.stopStream()
  if conn.replInCallback and not stopped.finished:
    # Waiting would deadlock when the caller is the callback itself.
    return
  await stopped

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
  ## ``slotName = ""`` streams without a slot. Non-zero ``timeline`` is sent as
  ## ``TIMELINE n`` (negative raises ``ValueError``). ``statusInterval`` behaves
  ## as on ``startReplication``.
  ##
  ## On a timeline switch the server may send a result set describing the next
  ## timeline between ``CopyDone`` and ``ReadyForQuery``; this proc drains and
  ## discards it — re-issue ``IDENTIFY_SYSTEM`` if you need that information.
  ##
  ## **Synchronous standbys:** the flush LSN governs how much WAL the primary may
  ## recycle, so a standby in ``synchronous_standby_names`` that relies on the
  ## auto-reply must call ``confirmFlushed`` (or reply manually) or the primary's
  ## ``COMMIT``s block waiting on a flush position that never advances.
  if timeline < 0:
    raise newException(ValueError, "timeline must be >= 0, got " & $timeline)
  conn.checkReady()

  var sql = "START_REPLICATION"
  if slotName.len > 0:
    sql.add(" SLOT " & quoteIdentifier(slotName))
  sql.add(" PHYSICAL " & $startLsn)
  if timeline > 0:
    sql.add(" TIMELINE " & $timeline)

  let msg = encodeQuery(sql)
  conn.markBusy()
  await conn.sendMsg(msg)
  await runReplicationStream(
    conn, startLsn, autoKeepaliveReply, statusInterval, rskPhysical, callback
  )
