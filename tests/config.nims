const asyncBackend {.strdefine.} = "asyncdispatch"

when asyncBackend == "asyncdispatch":
  switch("d", "ssl")

# Compile the connection-state transition guards into every test binary; the
# library ships without them.
switch("d", "pgStateChecks")
