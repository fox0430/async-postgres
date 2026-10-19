## Compile-time probe, imported first by `test_aggregate`: a module that
## imports `toPgBinaryParam` or `pgParams` alone must still encode every
## registered array type, so the generic `seq[T]` / `Option[T]` forms they
## reach cannot lean on the caller's `toPgParam`. Generic instances are shared
## across modules, so this must compile before any other module instantiates
## them.

import std/[macrocache, macros]
from std/options import Option

from ../async_postgres/pg_types import pgParams, toPgBinaryParam
from ../async_postgres/pg_types/encoding import pgArrayElemTypes

macro probeArrayParams(): untyped =
  result = newStmtList()
  for T in pgArrayElemTypes:
    let name = newLit(T.repr)
    result.add quote do:
      when not compiles(toPgBinaryParam(default(seq[`T`]))) or
          not compiles(toPgBinaryParam(default(Option[seq[`T`]]))):
        {.error: "toPgBinaryParam alone does not encode `" & `name` & "` arrays".}
      when not compiles(pgParams(default(Option[seq[`T`]]))):
        {.error: "pgParams alone does not encode `Option[seq[" & `name` & "]]`".}
      when not compiles(toPgBinaryParam(default(seq[Option[`T`]]))):
        {.
          error: "toPgBinaryParam alone does not encode `seq[Option[" & `name` & "]]`"
        .}
      when not compiles(pgParams(default(Option[seq[Option[`T`]]]))):
        {.
          error: "pgParams alone does not encode `Option[seq[Option[" & `name` & "]]]`"
        .}

probeArrayParams()
