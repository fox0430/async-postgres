import std/[unittest, options, strutils, math, deques, importutils, net]

import
  ../async_postgres/[async_backend, pg_protocol, pg_types, pg_client, pg_connection]

when hasChronos:
  import std/sets

import e2e_common

privateAccess(PgConnection)

suite "E2E: Cancel Request":
  test "cancel aborts pg_sleep":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Start a long-running query
      let sleepFut = conn.simpleQuery("SELECT pg_sleep(30)")

      # Give the server time to start executing
      await sleepAsync(milliseconds(100))

      # Cancel the query via a separate TCP connection
      await conn.cancel()

      # The original query should fail with query_canceled (57014)
      var raised = false
      try:
        discard await sleepFut
      except PgError as e:
        raised = true
        doAssert "57014" in e.msg
      doAssert raised

      # Connection should still be usable after cancel
      doAssert conn.state == csReady
      let res = await conn.simpleQuery("SELECT 1 AS check_col")
      doAssert res[0].rows[0][0].get().toString() == "1"

      await conn.close()

    waitFor t()

suite "E2E: Notice Callback":
  test "RAISE NOTICE triggers noticeCallback":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      var received: seq[Notice]
      conn.noticeCallback = proc(n: Notice) {.gcsafe, raises: [].} =
        received.add(n)

      discard await conn.exec("DO $$ BEGIN RAISE NOTICE 'hello from notice'; END $$")

      doAssert received.len == 1
      # Check that the message field ('M') contains our text
      var foundMsg = false
      for f in received[0].fields:
        if f.code == 'M':
          doAssert f.value == "hello from notice"
          foundMsg = true
      doAssert foundMsg

      await conn.close()

    waitFor t()

  test "notice callback not set does not interfere":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # No noticeCallback set — should not hang or error
      discard await conn.exec("DO $$ BEGIN RAISE NOTICE 'ignored'; END $$")
      let res = await conn.query("SELECT 1 AS check_col")
      doAssert res.rows.len == 1
      await conn.close()

    waitFor t()

suite "E2E: LISTEN/NOTIFY":
  test "basic notify and receive":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("test_chan")

      await sender.notify("test_chan", "hello")

      # Pump receives notification in background
      await sleepAsync(milliseconds(200))

      doAssert received.len == 1
      doAssert received[0].channel == "test_chan"
      doAssert received[0].payload == "hello"

      await listener.unlisten("test_chan")
      await listener.close()
      await sender.close()

    waitFor t()

  test "notify without payload":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("test_chan2")

      await sender.notify("test_chan2")

      await sleepAsync(milliseconds(200))

      doAssert received.len == 1
      doAssert received[0].channel == "test_chan2"
      doAssert received[0].payload == ""

      await listener.unlisten("test_chan2")
      await listener.close()
      await sender.close()

    waitFor t()

  test "multiple channels":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )

      await listener.listen("chan_a")
      await listener.listen("chan_b")

      await sender.notify("chan_a", "msg_a")
      await sender.notify("chan_b", "msg_b")

      await sleepAsync(milliseconds(200))

      doAssert received.len == 2
      doAssert received[0].channel == "chan_a"
      doAssert received[0].payload == "msg_a"
      doAssert received[1].channel == "chan_b"
      doAssert received[1].payload == "msg_b"

      await listener.unlisten("chan_a")
      await listener.unlisten("chan_b")
      await listener.close()
      await sender.close()

    waitFor t()

  test "unlisten stops notifications":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("test_unsub")

      await sender.notify("test_unsub", "before")
      # Pump is running in background, wait for delivery
      await sleepAsync(milliseconds(100))
      doAssert received.len == 1

      await listener.unlisten("test_unsub")

      await sender.notify("test_unsub", "after")
      await sleepAsync(milliseconds(100))
      doAssert received.len == 1

      await listener.close()
      await sender.close()

    waitFor t()

suite "E2E: Background LISTEN Pump":
  test "notification arrives without explicit query":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("bg_chan")

      # Send notification — pump should receive it without SELECT 1
      await sender.notify("bg_chan", "background")
      await sleepAsync(milliseconds(200))

      doAssert received.len == 1
      doAssert received[0].channel == "bg_chan"
      doAssert received[0].payload == "background"

      await listener.unlisten("bg_chan")
      await listener.close()
      await sender.close()

    waitFor t()

  test "multiple channels received in background":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )

      await listener.listen("bg_a")
      await listener.listen("bg_b")

      await sender.notify("bg_a", "msg_a")
      await sender.notify("bg_b", "msg_b")
      await sleepAsync(milliseconds(200))

      doAssert received.len == 2
      doAssert received[0].channel == "bg_a"
      doAssert received[1].channel == "bg_b"

      await listener.unlisten("bg_a")
      await listener.unlisten("bg_b")
      await listener.close()
      await sender.close()

    waitFor t()

  test "unlisten stops pump when no channels remain":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("bg_stop")

      await sender.notify("bg_stop", "before")
      await sleepAsync(milliseconds(100))
      doAssert received.len == 1

      await listener.unlisten("bg_stop")
      doAssert listener.state == csReady

      # Connection should be usable for queries after pump stops
      let res = await listener.simpleQuery("SELECT 1")
      doAssert res[0].rows[0][0].get().toString() == "1"

      await listener.close()
      await sender.close()

    waitFor t()

  test "close does not hang with active pump":
    proc t() {.async.} =
      let listener = await connect(plainConfig())

      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          discard
      )
      await listener.listen("bg_close")

      doAssert listener.state == csListening

      # close should cancel pump and not hang
      await listener.close()
      doAssert listener.state == csClosed

    waitFor t()

  test "unlisten partial channels keeps pump running":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )

      await listener.listen("partial_a")
      await listener.listen("partial_b")

      # Unlisten only one channel — pump should still be running for the other
      await listener.unlisten("partial_a")
      doAssert listener.state == csListening

      await sender.notify("partial_b", "still_alive")
      await sleepAsync(milliseconds(200))

      doAssert received.len == 1
      doAssert received[0].channel == "partial_b"
      doAssert received[0].payload == "still_alive"

      # Notification on unlistened channel should not arrive
      await sender.notify("partial_a", "should_not_arrive")
      await sleepAsync(milliseconds(100))
      doAssert received.len == 1

      await listener.unlisten("partial_b")
      await listener.close()
      await sender.close()

    waitFor t()

  test "listen after unlisten restarts pump":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var received: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          received.add(n)
      )
      await listener.listen("bg_restart")

      await sender.notify("bg_restart", "first")
      await sleepAsync(milliseconds(100))
      doAssert received.len == 1

      await listener.unlisten("bg_restart")
      doAssert listener.state == csReady

      # Re-listen should restart pump (callback is preserved)
      await listener.listen("bg_restart2")
      doAssert listener.state == csListening

      await sender.notify("bg_restart2", "second")
      await sleepAsync(milliseconds(200))
      doAssert received.len == 2
      doAssert received[1].channel == "bg_restart2"
      doAssert received[1].payload == "second"

      await listener.unlisten("bg_restart2")
      await listener.close()
      await sender.close()

    waitFor t()

suite "E2E: recvMessage Timeout":
  test "recvMessage with timeout succeeds on immediate response":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Send a simple query and receive with timeout
      await conn.sendMsg(encodeQuery("SELECT 1"))
      var gotRowData = false
      var gotComplete = false
      while true:
        let msg = await conn.recvMessage(timeout = seconds(5))
        case msg.kind
        of bmkRowDescription:
          discard
        of bmkDataRow:
          gotRowData = true
        of bmkCommandComplete:
          gotComplete = true
        of bmkReadyForQuery:
          break
        else:
          discard
      doAssert gotRowData
      doAssert gotComplete
      await conn.close()

    waitFor t()

  test "recvMessage with timeout raises AsyncTimeoutError when no data":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Don't send anything — recvMessage should timeout waiting for data
      var raised = false
      try:
        discard await conn.recvMessage(timeout = milliseconds(100))
      except AsyncTimeoutError:
        raised = true
      doAssert raised

    waitFor t()

  test "recvMessage buffer restored after timeout":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Logical unconsumed size (compactRecvBuf may shrink the raw buffer)
      let unconsumedBefore = conn.recvBuf.len - conn.recvBufStart
      # Trigger a timeout with no pending server data
      try:
        discard await conn.recvMessage(timeout = milliseconds(100))
      except AsyncTimeoutError:
        discard
      # recvBuf must not grow from the failed read
      let unconsumedAfter = conn.recvBuf.len - conn.recvBufStart
      doAssert unconsumedAfter == unconsumedBefore
      await conn.close()

    waitFor t()

  test "recvMessage timeout on large result set":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      # Start a query that produces a large result, then timeout mid-stream
      await conn.sendMsg(encodeQuery("SELECT generate_series(1, 100000)"))
      var raised = false
      try:
        # Very short timeout — unlikely to receive all messages in time
        while true:
          discard await conn.recvMessage(timeout = milliseconds(1))
      except AsyncTimeoutError:
        raised = true
      doAssert raised

    waitFor t()

  test "recvMessage without timeout (default) does not raise on normal message":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      await conn.sendMsg(encodeQuery("SELECT 42"))
      var value = ""
      while true:
        let msg = await conn.recvMessage() # default: no timeout
        case msg.kind
        of bmkDataRow:
          if msg.columns.len > 0 and msg.columns[0].isSome:
            value = cast[string](msg.columns[0].get())
        of bmkReadyForQuery:
          break
        else:
          discard
      doAssert value == "42"
      await conn.close()

    waitFor t()

suite "E2E: Notification Buffering":
  test "notifications buffered without callback":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      # No callback set — notifications should still be buffered
      await listener.listen("buf_nocb")

      await sender.notify("buf_nocb", "msg1")
      await sender.notify("buf_nocb", "msg2")
      await sleepAsync(milliseconds(200))

      doAssert listener.notifyQueue.len == 2
      let n1 = listener.notifyQueue.popFirst()
      doAssert n1.channel == "buf_nocb"
      doAssert n1.payload == "msg1"
      let n2 = listener.notifyQueue.popFirst()
      doAssert n2.channel == "buf_nocb"
      doAssert n2.payload == "msg2"

      await listener.unlisten("buf_nocb")
      await listener.close()
      await sender.close()

    waitFor t()

  test "notifications buffered alongside callback":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      var cbReceived: seq[Notification]
      listener.onNotify(
        proc(n: Notification) {.gcsafe, raises: [].} =
          cbReceived.add(n)
      )
      await listener.listen("buf_both")

      await sender.notify("buf_both", "hello")
      await sleepAsync(milliseconds(200))

      # Both callback and queue should have the notification
      doAssert cbReceived.len == 1
      doAssert listener.notifyQueue.len == 1
      doAssert listener.notifyQueue.peekFirst().payload == "hello"

      await listener.unlisten("buf_both")
      await listener.close()
      await sender.close()

    waitFor t()

  test "buffer respects max queue size and tracks drops":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      listener.notifyMaxQueue = 3
      await listener.listen("buf_max")

      for i in 1 .. 5:
        await sender.notify("buf_max", $i)
      await sleepAsync(milliseconds(300))

      # Only last 3 should remain (oldest dropped)
      doAssert listener.notifyQueue.len == 3
      doAssert listener.notifyDropped == 2

      # waitNotification should raise PgNotifyOverflowError
      var caught = false
      try:
        discard await listener.waitNotification()
      except PgNotifyOverflowError as e:
        caught = true
        doAssert e.dropped == 2
      doAssert caught

      # After overflow is cleared, normal access works
      doAssert listener.notifyDropped == 0
      let n = await listener.waitNotification()
      doAssert n.payload == "3"
      doAssert listener.notifyQueue.popFirst().payload == "4"
      doAssert listener.notifyQueue.popFirst().payload == "5"

      await listener.unlisten("buf_max")
      await listener.close()
      await sender.close()

    waitFor t()

  test "notifyMaxQueue <= 0 means an unbounded queue (no drops)":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      listener.notifyMaxQueue = 0 # unbounded
      await listener.listen("buf_unbounded")

      for i in 1 .. 20:
        await sender.notify("buf_unbounded", $i)
      await sleepAsync(milliseconds(300))

      # All retained in order, nothing dropped despite exceeding any small cap
      doAssert listener.notifyQueue.len == 20
      doAssert listener.notifyDropped == 0
      let n = await listener.waitNotification()
      doAssert n.payload == "1"

      await listener.unlisten("buf_unbounded")
      await listener.close()
      await sender.close()

    waitFor t()

  test "notifyMaxQueue == 0 still wakes a pending waitNotification":
    # Regression: with notifyMaxQueue == 0 the queue and waiter were gated
    # behind `> 0`, so waitNotification blocked forever even as notifications
    # arrived. Unbounded must still enqueue and complete the waiter.
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      listener.notifyMaxQueue = 0 # unbounded
      await listener.listen("buf_unbounded_wait")

      # Start waiting before any notification exists: exercises the
      # notifyWaiter.complete() path, not the fast queue-hit path.
      let waitFut = listener.waitNotification()
      await sleepAsync(milliseconds(50))
      doAssert not waitFut.finished

      await sender.notify("buf_unbounded_wait", "woke")
      await sleepAsync(milliseconds(200))

      doAssert waitFut.finished
      let n = await waitFut
      doAssert n.payload == "woke"

      await listener.unlisten("buf_unbounded_wait")
      await listener.close()
      await sender.close()

    waitFor t()

  test "notifyOverflowCallback fires on drop":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      listener.notifyMaxQueue = 2
      var cbDropped = 0
      listener.notifyOverflowCallback = proc(dropped: int) {.gcsafe, raises: [].} =
        cbDropped += dropped

      await listener.listen("buf_cb")

      for i in 1 .. 4:
        await sender.notify("buf_cb", $i)
      await sleepAsync(milliseconds(300))

      # 4 notifications into queue of 2: 2 dropped
      doAssert cbDropped == 2
      doAssert listener.notifyDropped == 2

      await listener.unlisten("buf_cb")
      await listener.close()
      await sender.close()

    waitFor t()

suite "E2E: waitNotification":
  test "waitNotification returns buffered notification immediately":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      await listener.listen("wait_imm")

      await sender.notify("wait_imm", "instant")
      await sleepAsync(milliseconds(200))

      let notif = await listener.waitNotification()
      doAssert notif.channel == "wait_imm"
      doAssert notif.payload == "instant"
      doAssert listener.notifyQueue.len == 0

      await listener.unlisten("wait_imm")
      await listener.close()
      await sender.close()

    waitFor t()

  test "waitNotification blocks until notification arrives":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      await listener.listen("wait_block")

      # Start waiting before notification is sent
      let waitFut = listener.waitNotification()

      await sleepAsync(milliseconds(50))
      doAssert not waitFut.finished

      await sender.notify("wait_block", "delayed")
      await sleepAsync(milliseconds(200))

      doAssert waitFut.finished
      let notif = await waitFut
      doAssert notif.channel == "wait_block"
      doAssert notif.payload == "delayed"

      await listener.unlisten("wait_block")
      await listener.close()
      await sender.close()

    waitFor t()

  test "waitNotification with timeout raises on expiry":
    proc t() {.async.} =
      let listener = await connect(plainConfig())

      await listener.listen("wait_timeout")

      var raised = false
      try:
        discard await listener.waitNotification(timeout = milliseconds(100))
      except PgError as e:
        raised = true
        doAssert "timed out" in e.msg
      doAssert raised

      await listener.unlisten("wait_timeout")
      await listener.close()

    waitFor t()

  test "waitNotification drains queue in order":
    proc t() {.async.} =
      let listener = await connect(plainConfig())
      let sender = await connect(plainConfig())

      await listener.listen("wait_order")

      await sender.notify("wait_order", "first")
      await sender.notify("wait_order", "second")
      await sender.notify("wait_order", "third")
      await sleepAsync(milliseconds(200))

      let n1 = await listener.waitNotification()
      let n2 = await listener.waitNotification()
      let n3 = await listener.waitNotification()
      doAssert n1.payload == "first"
      doAssert n2.payload == "second"
      doAssert n3.payload == "third"

      await listener.unlisten("wait_order")
      await listener.close()
      await sender.close()

    waitFor t()

when hasChronos:
  suite "E2E: LISTEN/NOTIFY Auto-Reconnect":
    test "reconnectInPlace restores connection and re-subscribes":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        let sender = await connect(plainConfig())

        var received: seq[Notification]
        listener.onNotify(
          proc(n: Notification) {.gcsafe, raises: [].} =
            received.add(n)
        )
        await listener.listen("reconn_manual")

        # Send before reconnect
        await sender.notify("reconn_manual", "before")
        await sleepAsync(milliseconds(200))
        doAssert received.len == 1

        # Force reconnect
        await listener.stopListening()
        await listener.reconnectInPlace()
        doAssert listener.state == csReady
        doAssert sets.contains(listener.listenChannels, "reconn_manual")

        # Channel was re-LISTENed by reconnectInPlace, start pump again
        listener.state = csListening
        listener.listenTask = listener.listenPump()

        await sender.notify("reconn_manual", "after")
        await sleepAsync(milliseconds(200))
        doAssert received.len == 2
        doAssert received[1].payload == "after"

        await listener.close()
        await sender.close()

      waitFor t()

    test "reconnectCallback is invoked on auto-reconnect":
      proc t() {.async.} =
        let listener = await connect(plainConfig())

        var reconnected = false
        listener.reconnectCallback = proc() {.gcsafe, raises: [].} =
          reconnected = true

        await listener.listen("reconn_cb")

        # Kill the connection from the server side
        let killer = await connect(plainConfig())
        try:
          discard await killer.exec(
            "SELECT pg_terminate_backend($1)", @[toPgParam(listener.pid)]
          )
        except PgError:
          discard
        await killer.close()

        # Wait for reconnect (backoff starts at 1s)
        await sleepAsync(milliseconds(3000))

        doAssert reconnected
        doAssert listener.state == csListening

        # Verify notifications still work after reconnect
        let sender = await connect(plainConfig())
        var received: seq[Notification]
        listener.onNotify(
          proc(n: Notification) {.gcsafe, raises: [].} =
            received.add(n)
        )

        await sender.notify("reconn_cb", "after_reconnect")
        await sleepAsync(milliseconds(200))
        doAssert received.len == 1
        doAssert received[0].payload == "after_reconnect"

        await listener.close()
        await sender.close()

      waitFor t()

    test "close during reconnect does not hang":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        await listener.listen("reconn_close")

        # Kill the connection
        let killer = await connect(plainConfig())
        try:
          discard await killer.exec(
            "SELECT pg_terminate_backend($1)", @[toPgParam(listener.pid)]
          )
        except PgError:
          discard
        await killer.close()

        # Give pump time to detect the failure and start reconnecting
        await sleepAsync(milliseconds(500))

        # Close should not hang even if reconnect is in progress
        await listener.close().wait(seconds(5))
        doAssert listener.state == csClosed

      waitFor t()

    test "listenReconnect config defaults":
      proc t() {.async.} =
        let conn = await connect(plainConfig())
        doAssert conn.listenReconnectMaxAttempts == 10
        doAssert conn.listenReconnectMaxBackoff == 30
        await conn.close()

      waitFor t()

    test "listenReconnect config setters":
      proc t() {.async.} =
        let conn = await connect(plainConfig())
        conn.listenReconnectMaxAttempts = 3
        conn.listenReconnectMaxBackoff = 5
        doAssert conn.listenReconnectMaxAttempts == 3
        doAssert conn.listenReconnectMaxBackoff == 5
        # 0 = unlimited (sentinel)
        conn.listenReconnectMaxAttempts = 0
        doAssert conn.listenReconnectMaxAttempts == 0
        await conn.close()

      waitFor t()

    test "auto-reconnect honors custom maxAttempts setting":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        listener.listenReconnectMaxAttempts = 2
        listener.listenReconnectMaxBackoff = 1

        var reconnected = false
        listener.reconnectCallback = proc() {.gcsafe, raises: [].} =
          reconnected = true

        await listener.listen("reconn_custom")

        let killer = await connect(plainConfig())
        try:
          discard await killer.exec(
            "SELECT pg_terminate_backend($1)", @[toPgParam(listener.pid)]
          )
        except PgError:
          discard
        await killer.close()

        # First retry runs after backoff=1s; reconnect should succeed.
        await sleepAsync(milliseconds(3000))
        doAssert reconnected
        doAssert listener.state == csListening

        await listener.close()

      waitFor t()

    test "auto-reconnect with unlimited attempts (maxAttempts=0)":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        listener.listenReconnectMaxAttempts = 0 # unlimited
        listener.listenReconnectMaxBackoff = 1

        var reconnected = false
        listener.reconnectCallback = proc() {.gcsafe, raises: [].} =
          reconnected = true

        await listener.listen("reconn_unlimited")

        let killer = await connect(plainConfig())
        try:
          discard await killer.exec(
            "SELECT pg_terminate_backend($1)", @[toPgParam(listener.pid)]
          )
        except PgError:
          discard
        await killer.close()

        await sleepAsync(milliseconds(3000))
        doAssert reconnected
        doAssert listener.state == csListening

        await listener.close()

      waitFor t()

    test "onListenError fires when reconnection fails permanently":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        listener.listenReconnectMaxAttempts = 1
        listener.listenReconnectMaxBackoff = 1

        var errored = false
        var errMsg = ""
        var reconnectionAttempted = false
        listener.onListenError(
          proc(err: ref PgListenError) {.gcsafe, raises: [].} =
            errored = true
            errMsg = err.msg
            reconnectionAttempted = err.reconnectionAttempted
        )

        await listener.listen("listen_err_cb")

        # Make reconnection fail: point the stored config at an unreachable port
        # so reconnectInPlace's connect() cannot succeed.
        listener.config.port = 1

        # Kill the backend so the pump's recv fails and triggers reconnect.
        let killer = await connect(plainConfig())
        try:
          discard await killer.exec(
            "SELECT pg_terminate_backend($1)", @[toPgParam(listener.pid)]
          )
        except PgError:
          discard
        await killer.close()

        # backoff=1s, then the single reconnect attempt fails → permanent death.
        await sleepAsync(milliseconds(4000))

        doAssert errored
        doAssert errMsg.len > 0
        doAssert "reconnection failed" in errMsg
        doAssert reconnectionAttempted
        doAssert listener.state == csClosed
        doAssert listener.listenError != nil
        doAssert listener.listenError.reconnectionAttempted

        await listener.close()

      waitFor t()

    test "waitNotification raises fresh PgListenError after permanent death":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        listener.listenReconnectMaxAttempts = 1
        listener.listenReconnectMaxBackoff = 1

        await listener.listen("listen_err_fresh")

        # Make reconnection fail, then kill the backend so the pump dies for good.
        listener.config.port = 1
        let killer = await connect(plainConfig())
        try:
          discard await killer.exec(
            "SELECT pg_terminate_backend($1)", @[toPgParam(listener.pid)]
          )
        except PgError:
          discard
        await killer.close()

        # backoff=1s, then the single reconnect attempt fails → permanent death.
        await sleepAsync(milliseconds(4000))
        doAssert listener.listenError != nil

        # The pull API surfaces the structured PgListenError, not a bare PgError.
        var first: ref PgListenError = nil
        try:
          discard await listener.waitNotification()
        except PgListenError as e:
          first = e
        doAssert first != nil
        doAssert first.reconnectionAttempted
        doAssert "reconnection failed" in first.msg
        # Each raise is a fresh instance, never the stored object — re-raising a
        # single shared ref would let its stack trace accumulate across calls.
        doAssert first != listener.listenError

        var second: ref PgListenError = nil
        try:
          discard await listener.waitNotification()
        except PgListenError as e:
          second = e
        doAssert second != nil
        doAssert second != first
        doAssert second != listener.listenError

        await listener.close()

      waitFor t()

    test "waitNotification fails on close":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        await listener.listen("wait_close")

        let waitFut = listener.waitNotification()
        await sleepAsync(milliseconds(50))
        doAssert not waitFut.finished

        await listener.close()

        var raised = false
        try:
          discard await waitFut
        except PgError:
          raised = true
        doAssert raised

      waitFor t()

    test "concurrent waitNotification raises error":
      proc t() {.async.} =
        let listener = await connect(plainConfig())
        await listener.listen("wait_concurrent")

        let waitFut1 = listener.waitNotification()
        await sleepAsync(milliseconds(10))
        doAssert not waitFut1.finished

        var raised = false
        try:
          discard await listener.waitNotification()
        except PgError:
          raised = true
        doAssert raised

        # Clean up: complete the first waiter by sending a notification
        let sender = await connect(plainConfig())
        discard await sender.query("NOTIFY wait_concurrent, 'done'")
        let notif = await waitFut1
        doAssert notif.channel == "wait_concurrent"
        doAssert notif.payload == "done"

        await sender.close()
        await listener.close()

      waitFor t()

suite "E2E: cancelNoWait":
  test "cancelNoWait aborts pg_sleep":
    proc t() {.async.} =
      let conn = await connect(plainConfig())

      # Start a long-running query
      let sleepFut = conn.simpleQuery("SELECT pg_sleep(30)")

      # Give the server time to start executing
      await sleepAsync(milliseconds(100))

      # Cancel the query without waiting
      conn.cancelNoWait()

      # The original query should fail with query_canceled (57014)
      var raised = false
      try:
        discard await sleepFut
      except PgError as e:
        raised = true
        doAssert "57014" in e.msg
      doAssert raised

      # Connection should still be usable after cancel
      doAssert conn.state == csReady
      let res = await conn.simpleQuery("SELECT 1 AS check_col")
      doAssert res[0].rows[0][0].get().toString() == "1"

      await conn.close()

    waitFor t()

  test "tsvector roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgTsVector("'cat':1A 'dog':3")
      let res = await conn.query("SELECT $1::tsvector", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getTsVector(0)
      doAssert $got == "'cat':1A 'dog':3"
      await conn.close()

    waitFor t()

  test "to_tsvector function":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT to_tsvector('english', 'The fat cat sat on the mat')")
      doAssert res.rows.len == 1
      let v = res.rows[0].getTsVector(0)
      let s = $v
      doAssert "'cat'" in s
      doAssert "'fat'" in s
      doAssert "'mat'" in s
      doAssert "'sat'" in s
      await conn.close()

    waitFor t()

  test "tsquery roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let q = PgTsQuery("'fat' & 'rat'")
      let res = await conn.query("SELECT $1::tsquery", @[toPgParam(q)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getTsQuery(0)
      doAssert "'fat' & 'rat'" == $got
      await conn.close()

    waitFor t()

  test "full-text search with @@ operator":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query(
        "SELECT to_tsvector('english', 'the fat cat') @@ to_tsquery('english', 'fat & cat')"
      )
      doAssert res.rows.len == 1
      doAssert res.rows[0].getBool(0) == true
      let res2 = await conn.query(
        "SELECT to_tsvector('english', 'the fat cat') @@ to_tsquery('english', 'fat & dog')"
      )
      doAssert res2.rows[0].getBool(0) == false
      await conn.close()

    waitFor t()

  test "NULL tsvector and tsquery":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::tsvector, NULL::tsquery")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getTsVectorOpt(0).isNone
      doAssert res.rows[0].getTsQueryOpt(1).isNone
      await conn.close()

    waitFor t()

  test "tsvector binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT 'cat:1A dog:3'::tsvector", resultFormat = rfBinary)
      doAssert res.rows.len == 1
      let v = res.rows[0].getTsVector(0)
      let s = $v
      doAssert "'cat'" in s
      doAssert "'dog'" in s
      await conn.close()

    waitFor t()

  test "tsquery binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT 'fat & rat'::tsquery", resultFormat = rfBinary)
      doAssert res.rows.len == 1
      let q = res.rows[0].getTsQuery(0)
      let s = $q
      doAssert "'fat'" in s
      doAssert "'rat'" in s
      await conn.close()

    waitFor t()

  test "xml roundtrip":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let v = PgXml("<root><item>hello</item></root>")
      let res = await conn.query("SELECT $1::xml", @[toPgParam(v)])
      doAssert res.rows.len == 1
      let got = res.rows[0].getXml(0)
      doAssert $got == "<root><item>hello</item></root>"
      await conn.close()

    waitFor t()

  test "xmlparse function":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT xmlparse(CONTENT '<item>test</item>')")
      doAssert res.rows.len == 1
      let v = res.rows[0].getXml(0)
      doAssert "<item>test</item>" == $v
      await conn.close()

    waitFor t()

  test "NULL xml":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res = await conn.query("SELECT NULL::xml")
      doAssert res.rows.len == 1
      doAssert res.rows[0].getXmlOpt(0).isNone
      await conn.close()

    waitFor t()

  test "xml binary results":
    proc t() {.async.} =
      let conn = await connect(plainConfig())
      let res =
        await conn.query("SELECT '<root>data</root>'::xml", resultFormat = rfBinary)
      doAssert res.rows.len == 1
      let v = res.rows[0].getXml(0)
      doAssert "<root>data</root>" == $v
      await conn.close()

    waitFor t()
