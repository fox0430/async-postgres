import std/unittest

import ../async_postgres

# Compile-time probe: the aggregate `import ../async_postgres` must re-export
# the documented public API surface. Every symbol individually re-exported by
# `async_postgres.nim` / `pg_connection.nim` / `pg_client.nim` / `pg_types.nim`
# is probed here, plus a representative subset of the modules that are still
# re-exported wholesale (`pg_pool_cluster` / `pg_largeobject` /
# `pg_advisory_lock` / `pg_sql` / `pg_replication` / `pg_auth` / `async_backend`
# and the `pg_types` submodules `core` / `array` / `user_types` / `accessors` /
# `ranges`). A forgotten whitelist entry fails the build instead of surfacing
# downstream. The wholesale-module subset is not exhaustive: a symbol dropped
# from a wholesale module outside this list stays undetected — extend the
# probes when narrowing those modules.
#
# The existence check is scoped to the aggregate module
# (`async_postgres.<name>`) rather than the bare `declared(name)`. A bare name
# also resolves to std/system symbols (`close`, `items`, `len`, `reset`, ...),
# so it would stay green even if the aggregate stopped re-exporting them.
# Qualified lookup only sees the aggregate's own exported surface, so these
# std-collision names are guarded just like everything else.
#
# The check is name visibility only: a whitelist entry re-exports every
# overload of a symbol at once, so dropping a *single* overload inside a
# submodule (rather than the whole name) is not caught by this probe. The same
# caveat applies to the `nameAccessor`-generated getter families below: an
# index-based overload under the same name keeps the probe green even if the
# name-based overload is removed. The probes still catch a family row that
# disappears entirely (macro table edit or module narrowing).
template apiExists(name: untyped) =
  when not declared(async_postgres.`name`):
    {.error: "aggregate import does not expose `" & astToStr(name) & "`".}

# -- connection types
apiExists(PgConnection)
apiExists(ConnConfig)
apiExists(SslMode)
apiExists(SslNegotiation)
apiExists(ChannelBindingMode)
apiExists(AuthMethod)
apiExists(TargetSessionAttrs)
apiExists(LoadBalanceHosts)
apiExists(HostEntry)
apiExists(PgConnState)
apiExists(state)
apiExists(PgTracer)
apiExists(Notification)
apiExists(Notice)
apiExists(CachedStmt)
apiExists(QueryResult)
apiExists(CopyResult)
apiExists(CopyOutInfo)
apiExists(CopyInInfo)
apiExists(CopyOutCallback)
apiExists(CopyInCallback)
apiExists(PgPoolOwner)
apiExists(RowCallback)
apiExists(ClientCertPairingErrorMsg)
apiExists(TypeOidInfo)
apiExists(ReconnectCallback)
apiExists(NotifyOverflowCallback)
apiExists(ListenErrorCallback)

# -- protocol types
apiExists(FrontendMessageKind)
apiExists(BackendMessageKind)
apiExists(DescribeKind)
apiExists(TransactionStatus)
apiExists(FieldDescription)
apiExists(CopyFormat)
apiExists(BackendMessage)
apiExists(ParseState)
apiExists(ParseResult)
apiExists(RowData)
apiExists(Row)

# -- pool API
apiExists(PgPool)
apiExists(PoolConfig)
apiExists(PoolMetrics)
apiExists(PooledConnHandle)

# -- client API
apiExists(PreparedStatement)
apiExists(Pipeline)
apiExists(PipelineResult)
apiExists(PipelineResultKind)
apiExists(IsolatedPipelineResults)
apiExists(Cursor)
apiExists(IsolationLevel)
apiExists(AccessMode)
apiExists(DeferrableMode)
apiExists(TransactionOptions)
apiExists(RetryOptions)

# -- wrapper / feature modules
apiExists(SqlQuery)
apiExists(Lsn)
apiExists(ReplicationCallback)
apiExists(ReplicaFallback)

# -- pg_types values
apiExists(PgParam)
apiExists(PgParamInline)
apiExists(PgUuid)
apiExists(PgInterval)
apiExists(PgMoney)
apiExists(initPgMoney)
apiExists(PgNumeric)
apiExists(PgInet)
apiExists(PgCidr)
apiExists(PgMacAddr)
apiExists(PgMacAddr8)
apiExists(PgHstore)
apiExists(PgPoint)
apiExists(PgPath)
apiExists(PgPolygon)
apiExists(PgBox)
apiExists(PgCircle)
apiExists(PgLseg)
apiExists(PgLine)
apiExists(PgBit)
apiExists(initPgBit)
apiExists(PgTime)
apiExists(PgTimeTz)
apiExists(PgXml)
apiExists(PgTsVector)
apiExists(PgTsQuery)
apiExists(PgRange)
apiExists(PgArray)

# -- connection entry points
apiExists(connect)
apiExists(simpleQuery)
apiExists(simpleExec)
apiExists(ping)
apiExists(cancel)
apiExists(cancelNoWait)
apiExists(invalidateOnTimeout)
apiExists(checkSessionAttrs)
apiExists(orderedHosts)
apiExists(connectToHost)
apiExists(isConnected)
apiExists(getHosts)
apiExists(isUnixSocket)
apiExists(unixSocketPath)
apiExists(socketHasFin)
apiExists(socketHasPendingData)

# -- listen / notify
apiExists(listen)
apiExists(unlisten)
apiExists(stopListening)
apiExists(onNotify)
apiExists(onListenError)
apiExists(onNotice)
apiExists(onReconnect)
apiExists(onNotifyOverflow)
apiExists(txStatus)
apiExists(pid)
apiExists(host)
apiExists(port)
apiExists(config)
apiExists(createdAt)
apiExists(sslEnabled)
apiExists(serverParams)
apiExists(serverParam)
apiExists(notifyDropped)
apiExists(listenError)
apiExists(notifyMaxQueue)
apiExists(listenReconnectMaxAttempts)
apiExists(listenReconnectMaxBackoff)
apiExists(stmtCacheCapacity)
apiExists(waitNotification)

# -- query / exec
apiExists(query)
apiExists(exec)
apiExists(queryEach)
apiExists(queryRow)
apiExists(queryRowOpt)
apiExists(queryValue)
apiExists(queryValueOpt)
apiExists(queryValueOrDefault)
apiExists(queryExists)
apiExists(queryColumn)
apiExists(notify)
apiExists(execInTransaction)
apiExists(queryInTransaction)
apiExists(buildBeginSql)
apiExists(isRetryableTxError)
apiExists(backoffDelayMs)

# -- prepared statements / cursors
apiExists(prepare)
apiExists(execute)
apiExists(columnIndex)
apiExists(fetchNext)
apiExists(openCursor)

# -- COPY
apiExists(copyIn)
apiExists(copyOut)
apiExists(copyInStream)
apiExists(copyOutStream)

# -- pool operations
apiExists(newPool)
apiExists(acquire)
apiExists(acquireHandle)
apiExists(release)
apiExists(runAndRelease)
apiExists(resetSession)
apiExists(resetSessionAndRelease)
apiExists(idleCount)
apiExists(activeCount)
apiExists(size)
apiExists(isClosed)
apiExists(metrics)
apiExists(initPoolConfig)
apiExists(close)

# -- DSN
apiExists(initConnConfig)
apiExists(parseDsn)

# -- pipeline
apiExists(newPipeline)
apiExists(addExec)
apiExists(addQuery)
apiExists(executeIsolated)
apiExists(reset)

# -- type conversion and accessors
apiExists(toPgParam)
apiExists(toPgParamInline)
apiExists(toPgBinaryParam)
apiExists(pgParams)
apiExists(fromPgText)
apiExists(parseTimestampText)
apiExists(parseDateText)
apiExists(parseTimeText)
apiExists(parseTimeTzText)
apiExists(parseHstoreText)
apiExists(parseIntervalText)
apiExists(parseInetText)
apiExists(parsePointText)
apiExists(parsePointsText)
apiExists(parseTextArray)
apiExists(getStr)
apiExists(getInt)
apiExists(getBool)
apiExists(getBytes)
apiExists(getJson)
apiExists(getUuid)
apiExists(getNumeric)
apiExists(getMoney)
apiExists(getTimestamp)
apiExists(getDate)
apiExists(getTime)
apiExists(getInterval)
apiExists(getInet)
apiExists(getCidr)
apiExists(getHstore)
apiExists(getPoint)
apiExists(getBox)
apiExists(getCircle)
apiExists(isNull)

# -- range helpers
apiExists(emptyRange)
apiExists(rangeOf)
apiExists(rangeFrom)
apiExists(rangeTo)
apiExists(unboundedRange)
apiExists(parseRangeText)

# -- query-result helpers
apiExists(quoteIdentifier)
apiExists(dialAddr)
apiExists(displayHost)
apiExists(lookupTypeOids)

# -- macros / templates that must stay reachable in user scope
apiExists(withTransaction)
apiExists(withTransactionRetry)
apiExists(withSavepoint)
apiExists(withTransactionDeadline)
apiExists(withTransactionRetryDeadline)
apiExists(withSavepointDeadline)
apiExists(withConnection)
apiExists(withPipeline)
apiExists(withCursor)
apiExists(queryDirect)
apiExists(execDirect)
apiExists(sql)
apiExists(withAdvisoryLock)
apiExists(withAdvisoryLockShared)
apiExists(withLargeObject)
apiExists(makeCopyInCallback)
apiExists(makeCopyOutCallback)
apiExists(withTracing)
apiExists(withConnTracing)

# -- protocol send-buffer writers (pg_protocol)
apiExists(addInt16)
apiExists(addInt32)
apiExists(addLen32)
apiExists(addCount16)
apiExists(addCString)
apiExists(addParse)
apiExists(addBind)
apiExists(addBindRaw)
apiExists(addDescribe)
apiExists(addExecute)
apiExists(addClose)
apiExists(addSync)
apiExists(addFlush)
apiExists(addCopyDone)
apiExists(addCopyBinaryHeader)
apiExists(addCopyBinaryTrailer)
apiExists(addCopyTupleStart)
apiExists(addCopyFieldNull)
apiExists(addCopyFieldInt16)
apiExists(addCopyFieldInt32)
apiExists(addCopyFieldInt64)
apiExists(addCopyFieldFloat32)
apiExists(addCopyFieldFloat64)
apiExists(addCopyFieldBool)
apiExists(addCopyFieldText)
apiExists(addCopyFieldString)
apiExists(patchMsgLen)
apiExists(patchMsgLenAtomic)
apiExists(preflightParseDirect)
apiExists(preflightBindDirect)
apiExists(syncMsg)
apiExists(flushMsg)
apiExists(copyDoneMsg)

# -- protocol message encoders / parse helpers (pg_protocol)
apiExists(encodeStartup)
apiExists(encodeSSLRequest)
apiExists(encodePassword)
apiExists(encodeSASLInitialResponse)
apiExists(encodeSASLResponse)
apiExists(encodeQuery)
apiExists(encodeParse)
apiExists(encodeBind)
apiExists(encodeDescribe)
apiExists(encodeExecute)
apiExists(encodeClose)
apiExists(encodeSync)
apiExists(encodeFlush)
apiExists(encodeTerminate)
apiExists(encodeCancelRequest)
apiExists(encodeCopyData)
apiExists(encodeCopyDone)
apiExists(encodeCopyFail)
apiExists(encodeStandbyStatusUpdate)
apiExists(buildResultFormats)
apiExists(parseDataRowInto)
apiExists(parseBackendMessage)
apiExists(formatError)
apiExists(isBinarySafeOid)
apiExists(BinarySafeOids)
apiExists(maxInt32Len)
apiExists(DefaultMaxBackendMessageLen)
apiExists(MaxNegotiateProtocolOptions)
apiExists(MaxErrorOrNoticeFields)
apiExists(MaxSaslMechanisms)

# -- row / QueryResult helpers (pg_protocol, simple_query)
apiExists(initRow)
apiExists(rowIdx)
apiExists(data)
apiExists(clone)
apiExists(newRowData)
apiExists(reuseRowData)
apiExists(rows)
apiExists(items)
apiExists(len)

# -- encoding binary-param helpers (pg_types/encoding)
apiExists(toPgDateParam)
apiExists(toPgTimestampTzParam)
apiExists(toPgBinaryDateParam)
apiExists(toPgBinaryTimestampTzParam)
apiExists(toPgTimestampArrayParam)
apiExists(toPgTimestampTzArrayParam)
apiExists(toPgDateArrayParam)
apiExists(toPgMoneyArrayParam)
apiExists(toPgMoneyArrayNDParam)
apiExists(toPgByteaArrayParam)
apiExists(encodeBinaryArray)
apiExists(encodeHstoreText)
apiExists(coerceBinaryParam)
apiExists(paramOidOf)
apiExists(addParseDirect)
apiExists(addBindDirect)
apiExists(foldBindParam)
apiExists(writeParamFormat)
apiExists(writeParamValue)
apiExists(writeParamOid)

# -- trace hook data types (pg_connection/types)
apiExists(TraceContext)
apiExists(TraceCopyDirection)
apiExists(TraceConnectStartData)
apiExists(TraceConnectEndData)
apiExists(TraceQueryStartData)
apiExists(TraceQueryEndData)
apiExists(TracePrepareStartData)
apiExists(TracePrepareEndData)
apiExists(TracePipelineStartData)
apiExists(TracePipelineEndData)
apiExists(TraceCopyStartData)
apiExists(TraceCopyEndData)
apiExists(TracePoolAcquireStartData)
apiExists(TracePoolAcquireEndData)
apiExists(TracePoolReleaseStartData)
apiExists(TracePoolReleaseEndData)
apiExists(TracePoolDoubleReleaseData)
apiExists(TracePoolCloseErrorData)
apiExists(TraceTransportCloseErrorData)
apiExists(TraceCleanupSkippedData)
apiExists(TraceLeakedSessionLocksData)
apiExists(TraceInsecureAuthData)
apiExists(TraceDeprecatedAuthData)
apiExists(TraceAdvisoryUnlockFailedData)
apiExists(CleanupKind)
apiExists(CleanupSkipReason)
apiExists(TransportCloseStage)
apiExists(NoticeCallback)
apiExists(NotifyCallback)

# -- symbol bound into user scope by the transaction macros (pg_client/transaction)
apiExists(rollbackGrace)

# -- large object API (pg_largeobject, wholesale export)
apiExists(loCreate)
apiExists(loOpen)
apiExists(loClose)
apiExists(loRead)
apiExists(loReadAll)
apiExists(loReadAllDeadline)
apiExists(loWrite)
apiExists(loWriteAll)
apiExists(loWriteAllDeadline)
apiExists(loSeek)
apiExists(loTell)
apiExists(loSize)
apiExists(loSizeDeadline)
apiExists(loTruncate)
apiExists(loUnlink)
apiExists(loImport)
apiExists(loExport)
apiExists(loReadStream)
apiExists(loReadStreamDeadline)
apiExists(loWriteStream)
apiExists(loWriteStreamDeadline)
apiExists(makeLoReadCallback)
apiExists(makeLoWriteCallback)
apiExists(INV_READ)
apiExists(INV_WRITE)
apiExists(INV_READWRITE)

# -- advisory lock API (pg_advisory_lock, wholesale export)
apiExists(advisoryLock)
apiExists(advisoryLockShared)
apiExists(advisoryLockXact)
apiExists(advisoryLockXactShared)
apiExists(advisoryTryLock)
apiExists(advisoryTryLockShared)
apiExists(advisoryTryLockXact)
apiExists(advisoryTryLockXactShared)
apiExists(advisoryUnlock)
apiExists(advisoryUnlockShared)
apiExists(advisoryUnlockAll)
apiExists(withAdvisoryLockXact)
apiExists(withAdvisoryLockXactShared)

# -- replication API (pg_replication, wholesale export)
apiExists(connectReplication)
apiExists(startReplication)
apiExists(startPhysicalReplication)
apiExists(stopReplication)
apiExists(createReplicationSlot)
apiExists(dropReplicationSlot)
apiExists(readReplicationSlot)
apiExists(identifySystem)
apiExists(timelineHistory)
apiExists(currentPgTimestamp)
apiExists(decodePgOutput)
apiExists(parsePgOutputMessage)
apiExists(parseReplicationMessage)
apiExists(sendCopyData)
apiExists(sendStandbyStatus)
apiExists(parseLsn)
apiExists(toInt64)
apiExists(toUInt64)
apiExists(confirmedFlushLsn)
apiExists(confirmFlushed)
apiExists(receivedEndLsn)
apiExists(hasOldTuple)
apiExists(makeReplicationCallback)

# -- pool-cluster API (pg_pool_cluster, wholesale export)
apiExists(newPoolCluster)
apiExists(primaryPool)
apiExists(replicaPool)
apiExists(withReadConnection)
apiExists(withWriteConnection)
apiExists(readConnection)
apiExists(writeConnection)
apiExists(fallbackTimeout)
apiExists(onReadFallback)

# -- auth helpers (pg_auth, wholesale export)
apiExists(md5AuthHash)
apiExists(scramClientFirstMessage)
apiExists(scramClientFinalMessage)
apiExists(scramVerifyServerFinal)
apiExists(scramEscapeUsername)
apiExists(computeTlsServerEndpoint)
apiExists(ScramState)
apiExists(DefaultMaxScramIterations)

# -- backend switches and helpers (async_backend, wholesale export)
apiExists(hasChronos)
apiExists(hasAsyncDispatch)
apiExists(hasTls)
apiExists(remainingDeadlineDuration)

# -- query-builder helper (pg_sql, wholesale export)
apiExists(sqlParams)

# -- range / multirange parameters and text parse (pg_types/ranges, wholesale export)
apiExists(toMultirange)
apiExists(toPgRangeParam)
apiExists(toPgMultirangeParam)
apiExists(toPgDateRangeParam)
apiExists(toPgDateMultirangeParam)
apiExists(toPgDateRangeArrayParam)
apiExists(toPgDateMultirangeArrayParam)
apiExists(toPgTsMultirangeArrayParam)
apiExists(toPgTsTzRangeParam)
apiExists(toPgTsTzMultirangeParam)
apiExists(toPgTsTzRangeArrayParam)
apiExists(toPgTsTzMultirangeArrayParam)
apiExists(toPgBinaryDateRangeParam)
apiExists(toPgBinaryDateMultirangeParam)
apiExists(toPgBinaryDateRangeArrayParam)
apiExists(toPgBinaryDateMultirangeArrayParam)
apiExists(toPgBinaryTsTzRangeParam)
apiExists(toPgBinaryTsTzMultirangeParam)
apiExists(toPgBinaryTsTzRangeArrayParam)
apiExists(toPgBinaryTsTzMultirangeArrayParam)
apiExists(parseMultirangeText)

# -- user-defined type macros / getters (pg_types/user_types, wholesale export)
apiExists(pgEnum)
apiExists(pgComposite)
apiExists(pgDomain)
apiExists(getEnum)
apiExists(getEnumOpt)
apiExists(getEnumArray)
apiExists(getEnumArrayOpt)
apiExists(getEnumArrayElemOpt)
apiExists(getComposite)
apiExists(getCompositeOpt)
apiExists(getDomain)
apiExists(getDomainOpt)
apiExists(parseCompositeText)
apiExists(encodeCompositeText)
apiExists(encodeEnumTextArray)
apiExists(encodeBinaryComposite)

# -- name-based row accessors / range&multirange getters generated by `nameAccessor*`
# -- (pg_types.nim): a removed family row here fails the build.
# -- See the header comment for the index-overload caveat.
apiExists(getBit)
apiExists(getBitArray)
apiExists(getBitArrayOpt)
apiExists(getBitOpt)
apiExists(getBoolArray)
apiExists(getBoolArrayElemOpt)
apiExists(getBoolArrayElemOptOpt)
apiExists(getBoolArrayOpt)
apiExists(getBoolOpt)
apiExists(getBoxArray)
apiExists(getBoxArrayOpt)
apiExists(getBoxOpt)
apiExists(getBytesArray)
apiExists(getBytesArrayOpt)
apiExists(getBytesOpt)
apiExists(getCidrArray)
apiExists(getCidrArrayOpt)
apiExists(getCidrOpt)
apiExists(getCircleArray)
apiExists(getCircleArrayOpt)
apiExists(getCircleOpt)
apiExists(getDateArray)
apiExists(getDateArrayOpt)
apiExists(getDateMultirange)
apiExists(getDateMultirangeArray)
apiExists(getDateMultirangeArrayOpt)
apiExists(getDateMultirangeOpt)
apiExists(getDateOpt)
apiExists(getDateRange)
apiExists(getDateRangeArray)
apiExists(getDateRangeArrayOpt)
apiExists(getDateRangeOpt)
apiExists(getFloat)
apiExists(getFloat32)
apiExists(getFloat32Array)
apiExists(getFloat32ArrayElemOpt)
apiExists(getFloat32ArrayElemOptOpt)
apiExists(getFloat32ArrayOpt)
apiExists(getFloat32Opt)
apiExists(getFloatArray)
apiExists(getFloatArrayElemOpt)
apiExists(getFloatArrayElemOptOpt)
apiExists(getFloatArrayOpt)
apiExists(getFloatOpt)
apiExists(getHstoreArray)
apiExists(getHstoreArrayOpt)
apiExists(getHstoreOpt)
apiExists(getInetArray)
apiExists(getInetArrayOpt)
apiExists(getInetOpt)
apiExists(getInt16)
apiExists(getInt16Array)
apiExists(getInt16ArrayElemOpt)
apiExists(getInt16ArrayElemOptOpt)
apiExists(getInt16ArrayOpt)
apiExists(getInt16Opt)
apiExists(getInt4Multirange)
apiExists(getInt4MultirangeArray)
apiExists(getInt4MultirangeArrayOpt)
apiExists(getInt4MultirangeOpt)
apiExists(getInt4Range)
apiExists(getInt4RangeArray)
apiExists(getInt4RangeArrayOpt)
apiExists(getInt4RangeOpt)
apiExists(getInt64)
apiExists(getInt64Array)
apiExists(getInt64ArrayElemOpt)
apiExists(getInt64ArrayElemOptOpt)
apiExists(getInt64ArrayOpt)
apiExists(getInt64Opt)
apiExists(getInt8Multirange)
apiExists(getInt8MultirangeArray)
apiExists(getInt8MultirangeArrayOpt)
apiExists(getInt8MultirangeOpt)
apiExists(getInt8Range)
apiExists(getInt8RangeArray)
apiExists(getInt8RangeArrayOpt)
apiExists(getInt8RangeOpt)
apiExists(getIntArray)
apiExists(getIntArrayElemOpt)
apiExists(getIntArrayElemOptOpt)
apiExists(getIntArrayOpt)
apiExists(getIntOpt)
apiExists(getIntervalArray)
apiExists(getIntervalArrayOpt)
apiExists(getIntervalOpt)
apiExists(getJsonArray)
apiExists(getJsonArrayOpt)
apiExists(getJsonOpt)
apiExists(getLine)
apiExists(getLineArray)
apiExists(getLineArrayOpt)
apiExists(getLineOpt)
apiExists(getLseg)
apiExists(getLsegArray)
apiExists(getLsegArrayOpt)
apiExists(getLsegOpt)
apiExists(getMacAddr)
apiExists(getMacAddr8)
apiExists(getMacAddr8Array)
apiExists(getMacAddr8ArrayOpt)
apiExists(getMacAddr8Opt)
apiExists(getMacAddrArray)
apiExists(getMacAddrArrayOpt)
apiExists(getMacAddrOpt)
apiExists(getMoneyArray)
apiExists(getMoneyArrayND)
apiExists(getMoneyArrayNDOpt)
apiExists(getMoneyArrayOpt)
apiExists(getMoneyOpt)
apiExists(getNumMultirange)
apiExists(getNumMultirangeArray)
apiExists(getNumMultirangeArrayOpt)
apiExists(getNumMultirangeOpt)
apiExists(getNumRange)
apiExists(getNumRangeArray)
apiExists(getNumRangeArrayOpt)
apiExists(getNumRangeOpt)
apiExists(getNumericArray)
apiExists(getNumericArrayOpt)
apiExists(getNumericOpt)
apiExists(getPath)
apiExists(getPathArray)
apiExists(getPathArrayOpt)
apiExists(getPathOpt)
apiExists(getPointArray)
apiExists(getPointArrayOpt)
apiExists(getPointOpt)
apiExists(getPolygon)
apiExists(getPolygonArray)
apiExists(getPolygonArrayOpt)
apiExists(getPolygonOpt)
apiExists(getStrArray)
apiExists(getStrArrayElemOpt)
apiExists(getStrArrayElemOptOpt)
apiExists(getStrArrayOpt)
apiExists(getStrOpt)
apiExists(getTimeArray)
apiExists(getTimeArrayOpt)
apiExists(getTimeOpt)
apiExists(getTimeTz)
apiExists(getTimeTzArray)
apiExists(getTimeTzArrayOpt)
apiExists(getTimeTzOpt)
apiExists(getTimestampArray)
apiExists(getTimestampArrayOpt)
apiExists(getTimestampOpt)
apiExists(getTimestampTz)
apiExists(getTimestampTzArray)
apiExists(getTimestampTzArrayOpt)
apiExists(getTimestampTzOpt)
apiExists(getTsMultirange)
apiExists(getTsMultirangeArray)
apiExists(getTsMultirangeArrayOpt)
apiExists(getTsMultirangeOpt)
apiExists(getTsQuery)
apiExists(getTsQueryArray)
apiExists(getTsQueryArrayOpt)
apiExists(getTsQueryOpt)
apiExists(getTsRange)
apiExists(getTsRangeArray)
apiExists(getTsRangeArrayOpt)
apiExists(getTsRangeOpt)
apiExists(getTsTzMultirange)
apiExists(getTsTzMultirangeArray)
apiExists(getTsTzMultirangeArrayOpt)
apiExists(getTsTzMultirangeOpt)
apiExists(getTsTzRange)
apiExists(getTsTzRangeArray)
apiExists(getTsTzRangeArrayOpt)
apiExists(getTsTzRangeOpt)
apiExists(getTsVector)
apiExists(getTsVectorArray)
apiExists(getTsVectorArrayOpt)
apiExists(getTsVectorOpt)
apiExists(getUuidArray)
apiExists(getUuidArrayOpt)
apiExists(getUuidOpt)
apiExists(getXml)
apiExists(getXmlArray)
apiExists(getXmlArrayOpt)
apiExists(getXmlOpt)

# -- macro expansion probes
#
# `apiExists` above is name visibility only: a macro's *symbol* re-exports
# fine even when its generated code cannot compile in a scope that lacks
# `privateAccess(PgConnection)`. These procs instantiate every scoping macro
# from an ordinary user scope (this module deliberately never calls
# `privateAccess`), so a macro that reaches a private field without unlocking
# it fails the build here instead of only in `examples/`.
#
# The procs are compiled, never run — `conn` / `pool` are nil. `withTracing`
# and `withConnTracing` are not probed: they take tracer hook expressions and
# are internal plumbing rather than a user-facing scoping macro.

proc probeConnTx(conn: PgConnection) {.async, used.} =
  conn.withTransaction:
    discard await conn.exec("SELECT 1")
  conn.withTransaction(seconds(5)):
    discard await conn.exec("SELECT 1")
  conn.withTransaction(TransactionOptions(isolation: ilSerializable)):
    discard await conn.exec("SELECT 1")
  conn.withTransactionRetry(RetryOptions(maxAttempts: 3)):
    discard await conn.exec("SELECT 1")
  conn.withTransactionDeadline(seconds(5)):
    discard await conn.exec("SELECT 1")
  conn.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), seconds(5)):
    discard await conn.exec("SELECT 1")

proc probeConnSavepoint(conn: PgConnection) {.async, used.} =
  conn.withSavepoint:
    discard await conn.exec("SELECT 1")
  conn.withSavepoint("named_sp"):
    discard await conn.exec("SELECT 1")
  conn.withSavepointDeadline(seconds(5)):
    discard await conn.exec("SELECT 1")

proc probeMacroScopeStaysSealed(conn: PgConnection) {.async, used.} =
  ## The scoping macros' generated code drives the state machine and writes the
  ## send buffer, but it lands here, in the caller's scope. Unlocking the record
  ## for that made every private field of `PgConnection` writable for the rest
  ## of the enclosing proc — silently, and only for callers who happened to use
  ## a macro. Each of these must stay unreachable *after* an expansion.
  conn.withTransaction:
    discard await conn.exec("SELECT 1")
  conn.withSavepoint:
    discard await conn.exec("SELECT 1")
  discard await conn.execDirect("SELECT $1::int", 1)
  # `static:` because these probe procs are compiled and never run.
  static:
    doAssert not compiles(conn.sendBuf), "the send buffer must stay sealed"
    doAssert not compiles(conn.stmtCache), "the statement cache must stay sealed"
    doAssert not compiles(conn.portalCounter), "the portal counter must stay sealed"
    doAssert not compiles(conn.state = csClosed),
      "the state machine must stay read-only"

proc probeConnAccessors(conn: PgConnection) {.used.} =
  ## The record is private, so every documented read and every tunable has to
  ## reach application code through an accessor. `declared()` alone would stay
  ## green on a name that std or another module also defines, so use them.
  discard conn.pid
  discard conn.host
  discard conn.port
  discard conn.config
  discard conn.createdAt
  discard conn.sslEnabled
  discard conn.serverParams
  discard conn.serverParam("server_version")
  discard conn.notifyDropped
  discard conn.listenError
  discard conn.state
  discard conn.txStatus
  conn.notifyMaxQueue = conn.notifyMaxQueue
  conn.listenReconnectMaxAttempts = conn.listenReconnectMaxAttempts
  conn.listenReconnectMaxBackoff = conn.listenReconnectMaxBackoff
  conn.stmtCacheCapacity = conn.stmtCacheCapacity
  conn.onNotify(
    proc(n: Notification) {.gcsafe, raises: [].} =
      discard
  )
  conn.onNotice(
    proc(n: Notice) {.gcsafe, raises: [].} =
      discard
  )
  conn.onReconnect(
    proc() {.gcsafe, raises: [].} =
      discard
  )
  conn.onNotifyOverflow(
    proc(dropped: int) {.gcsafe, raises: [].} =
      discard
  )
  conn.onListenError(
    proc(err: ref PgListenError) {.gcsafe, raises: [].} =
      discard
  )

proc probePrivatizedSurfaceStaysSealed() {.used.} =
  ## Negative guards for the narrowed surface (affirmative `apiExists` alone
  ## cannot catch re-expansion).
  static:
    doAssert not compiles(PgMoney(amount: 1'i64, scale: 2'i8)),
      "PgMoney must stay constructible only via initPgMoney"
    doAssert not compiles(PgBit(nbits: 1'i32, data: @[0b10000000'u8])),
      "PgBit must stay constructible only via initPgBit"
    doAssert not compiles(async_postgres.cellInfo),
      "cellInfo must stay out of the public API"

proc probeConnScoped(conn: PgConnection, oid: Oid) {.async, used.} =
  conn.withAdvisoryLock(1'i64):
    discard await conn.exec("SELECT 1")
  conn.withAdvisoryLockShared(1'i64):
    discard await conn.exec("SELECT 1")
  conn.withLargeObject(lo, oid, INV_READ):
    discard await lo.loSizeDeadline(seconds(5))
  conn.withCursor("SELECT 1", 10'i32, cur):
    discard await cur.fetchNext()
  let qr = await conn.queryDirect("SELECT $1::int", 1)
  discard qr
  discard await conn.execDirect("SELECT $1::int", 1)

proc probePoolTx(pool: PgPool) {.async, used.} =
  # Each macro injects its connection identifier into this scope, so every
  # probe needs its own name.
  pool.withConnection(cConn):
    discard await cConn.exec("SELECT 1")
  pool.withPipeline(pl):
    # `conn` is injected by the macro alongside the pipeline.
    discard pl
    discard await conn.exec("SELECT 1")
  pool.withTransaction(cTx):
    discard await cTx.exec("SELECT 1")
  pool.withTransactionRetry(RetryOptions(maxAttempts: 3), cRetry):
    discard await cRetry.exec("SELECT 1")
  pool.withTransactionDeadline(cDl, seconds(5)):
    discard await cDl.exec("SELECT 1")
  pool.withTransactionRetryDeadline(RetryOptions(maxAttempts: 3), cRetryDl, seconds(5)):
    discard await cRetryDl.exec("SELECT 1")

proc probeClusterTx(cluster: PgPoolCluster) {.async, used.} =
  cluster.withReadConnection(cRead):
    discard await cRead.exec("SELECT 1")
  cluster.withWriteConnection(cWrite):
    discard await cWrite.exec("SELECT 1")
  cluster.withTransaction(cTx):
    discard await cTx.exec("SELECT 1")
  cluster.withTransactionRetry(RetryOptions(maxAttempts: 3), cRetry):
    discard await cRetry.exec("SELECT 1")
  cluster.withTransactionDeadline(cDl, seconds(5)):
    discard await cDl.exec("SELECT 1")
  cluster.withTransactionRetryDeadline(
    RetryOptions(maxAttempts: 3), cRetryDl, seconds(5)
  ):
    discard await cRetryDl.exec("SELECT 1")

suite "aggregate re-export":
  test "public API surface resolves through `import pkg/async_postgres`":
    check true
