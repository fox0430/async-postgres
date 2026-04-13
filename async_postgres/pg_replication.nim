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
    startLsn*: Lsn ## Start of the WAL data in this message
    endLsn*: Lsn ## Current end of WAL on the server
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

proc toString*(field: TupleField): string =
  ## Convert a TupleField's data to a string by copying the bytes.
  result = newString(field.data.len)
  if field.data.len > 0:
    copyMem(addr result[0], unsafeAddr field.data[0], field.data.len)

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

proc decodeCStringAt(buf: openArray[byte], offset: int): (string, int) =
  ## Decode a null-terminated string at offset. Returns (string, next offset).
  if offset >= buf.len:
    raise newException(ProtocolError, "decodeCStringAt: offset past end of buffer")
  var i = offset
  while i < buf.len and buf[i] != 0:
    inc i
  if i >= buf.len:
    raise newException(ProtocolError, "decodeCStringAt: missing null terminator")
  let slen = i - offset
  var s = newString(slen)
  if slen > 0:
    copyMem(addr s[0], unsafeAddr buf[offset], slen)
  inc i # skip null
  (s, i)

proc decodeTuple(buf: openArray[byte], offset: int): (seq[TupleField], int) =
  ## Decode a pgoutput TupleData structure.
  var pos = offset
  let numCols = decodeInt16(buf, pos)
  pos += 2
  var fields = newSeq[TupleField](numCols)
  for i in 0 ..< numCols:
    let kind = char(buf[pos])
    inc pos
    case kind
    of 'n':
      fields[i] = TupleField(kind: tdkNull)
    of 'u':
      fields[i] = TupleField(kind: tdkUnchanged)
    of 't', 'b':
      let dataLen = decodeInt32(buf, pos)
      pos += 4
      var data = newSeq[byte](dataLen)
      if dataLen > 0:
        copyMem(addr data[0], unsafeAddr buf[pos], dataLen)
      pos += int(dataLen)
      fields[i] = TupleField(kind: if kind == 't': tdkText else: tdkBinary, data: data)
    else:
      raise newException(ProtocolError, "Unknown tuple field kind: " & kind)
  (fields, pos)

proc parsePgOutputMessage*(data: openArray[byte]): PgOutputMessage =
  ## Decode a pgoutput logical decoding message from raw WAL bytes.
  if data.len == 0:
    raise newException(ProtocolError, "Empty pgoutput message")
  let msgType = char(data[0])
  case msgType
  of 'B': # Begin
    var msg = BeginMessage()
    msg.finalLsn = Lsn(cast[uint64](decodeInt64(data, 1)))
    msg.commitTime = decodeInt64(data, 9)
    msg.xid = decodeInt32(data, 17)
    PgOutputMessage(kind: pomkBegin, begin: msg)
  of 'C': # Commit
    var msg = CommitMessage()
    msg.flags = data[1]
    msg.commitLsn = Lsn(cast[uint64](decodeInt64(data, 2)))
    msg.endLsn = Lsn(cast[uint64](decodeInt64(data, 10)))
    msg.commitTime = decodeInt64(data, 18)
    PgOutputMessage(kind: pomkCommit, commit: msg)
  of 'O': # Origin
    var msg = OriginMessage()
    msg.originLsn = Lsn(cast[uint64](decodeInt64(data, 1)))
    let (name, _) = decodeCStringAt(data, 9)
    msg.originName = name
    PgOutputMessage(kind: pomkOrigin, origin: msg)
  of 'R': # Relation
    var msg = RelationInfo()
    msg.relationId = decodeInt32(data, 1)
    var pos = 5
    let (ns, pos2) = decodeCStringAt(data, pos)
    msg.namespace = ns
    pos = pos2
    let (name, pos3) = decodeCStringAt(data, pos)
    msg.name = name
    pos = pos3
    msg.replicaIdentity = char(data[pos])
    inc pos
    let numCols = decodeInt16(data, pos)
    pos += 2
    msg.columns = newSeq[RelationColumn](numCols)
    for i in 0 ..< numCols:
      var col = RelationColumn()
      col.flags = data[pos]
      inc pos
      let (colName, nextPos) = decodeCStringAt(data, pos)
      col.name = colName
      pos = nextPos
      col.typeOid = decodeInt32(data, pos)
      pos += 4
      col.typeMod = decodeInt32(data, pos)
      pos += 4
      msg.columns[i] = col
    PgOutputMessage(kind: pomkRelation, relation: msg)
  of 'Y': # Type
    var msg = TypeMessage()
    msg.typeId = decodeInt32(data, 1)
    var pos = 5
    let (ns, pos2) = decodeCStringAt(data, pos)
    msg.namespace = ns
    pos = pos2
    let (name, _) = decodeCStringAt(data, pos)
    msg.name = name
    PgOutputMessage(kind: pomkType, typeMsg: msg)
  of 'I': # Insert
    var msg = InsertMessage()
    msg.relationId = decodeInt32(data, 1)
    # byte at offset 5 is 'N' (new tuple marker)
    let (fields, _) = decodeTuple(data, 6)
    msg.newTuple = fields
    PgOutputMessage(kind: pomkInsert, insert: msg)
  of 'U': # Update
    var msg = UpdateMessage()
    msg.relationId = decodeInt32(data, 1)
    var pos = 5
    let marker = char(data[pos])
    inc pos
    if marker == 'K' or marker == 'O':
      # Old key or old tuple included
      msg.hasOldTuple = true
      let (oldFields, nextPos) = decodeTuple(data, pos)
      msg.oldTuple = oldFields
      pos = nextPos
      inc pos # skip 'N' marker for new tuple
    elif marker != 'N':
      raise newException(ProtocolError, "Unknown Update tuple marker: " & marker)
    let (newFields, _) = decodeTuple(data, pos)
    msg.newTuple = newFields
    PgOutputMessage(kind: pomkUpdate, update: msg)
  of 'D': # Delete
    var msg = DeleteMessage()
    msg.relationId = decodeInt32(data, 1)
    var pos = 5
    # byte at offset 5 is 'K' (key) or 'O' (old tuple)
    inc pos
    let (fields, _) = decodeTuple(data, pos)
    msg.oldTuple = fields
    PgOutputMessage(kind: pomkDelete, delete: msg)
  of 'T': # Truncate
    var msg = TruncateMessage()
    let numRels = decodeInt32(data, 1)
    msg.options = data[5]
    msg.relationIds = newSeq[int32](numRels)
    var pos = 6
    for i in 0 ..< numRels:
      msg.relationIds[i] = decodeInt32(data, pos)
      pos += 4
    PgOutputMessage(kind: pomkTruncate, truncate: msg)
  of 'M': # Message
    var msg = LogicalMessage()
    msg.flags = data[1]
    msg.lsn = Lsn(cast[uint64](decodeInt64(data, 2)))
    var pos = 10
    let (prefix, nextPos) = decodeCStringAt(data, pos)
    msg.prefix = prefix
    pos = nextPos
    let contentLen = decodeInt32(data, pos)
    pos += 4
    if contentLen > 0:
      msg.content = newSeq[byte](contentLen)
      copyMem(addr msg.content[0], unsafeAddr data[pos], contentLen)
    PgOutputMessage(kind: pomkMessage, message: msg)
  else:
    raise newException(ProtocolError, "Unknown pgoutput message type: " & msgType)

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

proc connectReplication*(config: ConnConfig): Future[PgConnection] =
  ## Connect to PostgreSQL with ``replication=database`` in startup parameters.
  ## This enables replication commands (IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT, etc.).
  var cfg = config
  cfg.extraParams.add(("replication", "database"))
  connect(cfg)

proc connectReplication*(dsn: string): Future[PgConnection] =
  ## Connect to PostgreSQL with ``replication=database`` using a DSN string.
  var cfg = parseDsn(dsn)
  cfg.extraParams.add(("replication", "database"))
  connect(cfg)

# Replication commands (via simple query protocol)

proc identifySystem*(conn: PgConnection): Future[SystemInfo] {.async.} =
  ## Execute ``IDENTIFY_SYSTEM`` and return system identification info.
  let results = await conn.simpleQuery("IDENTIFY_SYSTEM")
  if results.len == 0 or results[0].rowCount == 0:
    raise newException(PgConnectionError, "IDENTIFY_SYSTEM returned no results")
  let qr = results[0]
  let row = Row(data: qr.data, rowIdx: 0)
  var info = SystemInfo()
  info.systemId = row.getStr(0)
  info.timeline = parseInt(row.getStr(1)).int32
  info.xLogPos = parseLsn(row.getStr(2))
  if qr.fields.len > 3:
    info.dbName = row.getStr(3)
  return info

proc createReplicationSlot*(
    conn: PgConnection,
    slotName: string,
    plugin: string = "pgoutput",
    temporary: bool = false,
): Future[ReplicationSlotInfo] {.async.} =
  ## Create a logical replication slot. Returns slot info including the consistent point LSN.
  var sql = "CREATE_REPLICATION_SLOT " & quoteIdentifier(slotName)
  if temporary:
    sql.add(" TEMPORARY")
  sql.add(" LOGICAL " & quoteIdentifier(plugin))

  let results = await conn.simpleQuery(sql)
  if results.len == 0 or results[0].rowCount == 0:
    raise newException(PgConnectionError, "CREATE_REPLICATION_SLOT returned no results")
  let qr = results[0]
  let row = Row(data: qr.data, rowIdx: 0)
  var info = ReplicationSlotInfo()
  info.slotName = row.getStr(0)
  info.consistentPoint = parseLsn(row.getStr(1))
  if qr.fields.len > 2:
    info.snapshotName = row.getStr(2)
  if qr.fields.len > 3:
    info.outputPlugin = row.getStr(3)
  return info

proc dropReplicationSlot*(
    conn: PgConnection, slotName: string, wait: bool = false
): Future[void] {.async.} =
  ## Drop a replication slot.
  var sql = "DROP_REPLICATION_SLOT " & quoteIdentifier(slotName)
  if wait:
    sql.add(" WAIT")
  discard await conn.simpleQuery(sql)

proc readReplicationSlot*(
    conn: PgConnection, slotName: string
): Future[ReplicationSlotInfo] {.async.} =
  ## Read information about an existing replication slot.
  let results =
    await conn.simpleQuery("READ_REPLICATION_SLOT " & quoteIdentifier(slotName))
  if results.len == 0 or results[0].rowCount == 0:
    raise newException(PgConnectionError, "READ_REPLICATION_SLOT returned no results")
  let qr = results[0]
  let row = Row(data: qr.data, rowIdx: 0)
  var info = ReplicationSlotInfo()
  # READ_REPLICATION_SLOT returns: slot_type, restart_lsn, restart_tli
  # But the column layout depends on PG version. We handle common case.
  info.slotName = slotName
  if not row.isNull(1):
    info.consistentPoint = parseLsn(row.getStr(1))
  return info

# Replication streaming

proc parseReplicationMessage*(copyData: seq[byte]): ReplicationMessage =
  ## Parse a CopyData payload into a ReplicationMessage.
  if copyData.len == 0:
    raise newException(ProtocolError, "Empty replication CopyData")
  let kind = char(copyData[0])
  case kind
  of 'w': # XLogData
    if copyData.len < 25:
      raise newException(ProtocolError, "XLogData message too short")
    var xlog = XLogData()
    xlog.startLsn = Lsn(cast[uint64](decodeInt64(copyData, 1)))
    xlog.endLsn = Lsn(cast[uint64](decodeInt64(copyData, 9)))
    xlog.sendTime = decodeInt64(copyData, 17)
    let dataStart = 25
    if copyData.len > dataStart:
      xlog.data = copyData[dataStart ..< copyData.len]
    ReplicationMessage(kind: rmkXLogData, xlogData: xlog)
  of 'k': # Primary Keepalive
    if copyData.len < 18:
      raise newException(ProtocolError, "Primary Keepalive message too short")
    var ka = PrimaryKeepalive()
    ka.walEnd = Lsn(cast[uint64](decodeInt64(copyData, 1)))
    ka.sendTime = decodeInt64(copyData, 9)
    ka.replyRequested = copyData[17] != 0
    ReplicationMessage(kind: rmkPrimaryKeepalive, keepalive: ka)
  else:
    raise newException(ProtocolError, "Unknown replication message type: " & kind)

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
    callback: ReplicationCallback,
): Future[void] {.async.} =
  ## Begin logical replication streaming from the given slot.
  ##
  ## The ``callback`` is invoked for each ``XLogData`` or ``PrimaryKeepalive``
  ## message received. The callback is awaited, providing natural TCP backpressure.
  ## Within the callback, use ``sendStandbyStatus`` to acknowledge received data.
  ##
  ## The proc returns when the server sends ``CopyDone`` or the connection closes.
  ## To stop replication from the client side, call ``stopReplication`` from within
  ## the callback (or from a concurrent task).
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

  # Wait for CopyBothResponse
  var gotCopyBoth = false
  var queryError: ref PgQueryError

  block waitCopyBoth:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyBothResponse:
          gotCopyBoth = true
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

  if not gotCopyBoth:
    return

  # Streaming loop
  block recvLoop:
    while true:
      while (let opt = conn.nextMessage(); opt.isSome):
        let msg = opt.get
        case msg.kind
        of bmkCopyData:
          let replMsg = parseReplicationMessage(msg.copyData)
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
