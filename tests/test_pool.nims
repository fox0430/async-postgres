# Compiles the pre-flight call counter this suite's reachability test reads.
# Scoped here so every other test binary builds the library as production does.
switch("d", "pgTestObservability")
