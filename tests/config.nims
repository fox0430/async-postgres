const asyncBackend {.strdefine.} = "asyncdispatch"

when asyncBackend == "asyncdispatch":
  switch("d", "ssl")
