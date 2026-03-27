## LISTEN/NOTIFY example.
##
## Demonstrates PostgreSQL's asynchronous notification mechanism
## using two connections: one listens and one sends notifications.
##
## Usage:
##   nim c -d:ssl -r examples/listen_notify.nim

import pkg/async_postgres

const Dsn = "postgresql://test:test@127.0.0.1:15432/test?sslmode=disable"

proc main() {.async.} =
  # Listener connection
  let listener = await connect(Dsn)
  defer:
    await listener.close()

  # Sender connection (LISTEN puts a connection into listening mode,
  # so we need a separate connection for sending)
  let sender = await connect(Dsn)
  defer:
    await sender.close()

  await listener.listen("chat")
  echo "Listening on channel 'chat'..."

  # Send notifications from the other connection
  await sender.notify("chat", "Hello from sender!")
  await sender.notify("chat", "Second message")
  await sender.notify("chat", "done")

  # Receive notifications
  while true:
    let notification = await listener.waitNotification(timeout = seconds(5))
    echo "Received: channel=", notification.channel, " payload=", notification.payload
    if notification.payload == "done":
      break

  await listener.unlisten("chat")
  echo "Done."

waitFor main()
