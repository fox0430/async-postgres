import std/[unittest, options]

import ../async_postgres/pg_types {.all.}
import ../async_postgres/pg_sql

suite "sqlParams":
  test "basic conversion":
    check sqlParams("SELECT ? WHERE ?") == "SELECT $1 WHERE $2"

  test "single placeholder":
    check sqlParams("SELECT * FROM t WHERE id = ?") == "SELECT * FROM t WHERE id = $1"

  test "escape ?? to literal ?":
    check sqlParams("SELECT ??") == "SELECT ?"

  test "mixed escape and placeholder":
    check sqlParams("SELECT ?? WHERE id = ?") == "SELECT ? WHERE id = $1"

  test "? inside single-quoted string preserved":
    check sqlParams("SELECT 'what?' WHERE id = ?") == "SELECT 'what?' WHERE id = $1"

  test "escaped quote in string":
    check sqlParams("SELECT 'it''s ?' WHERE id = ?") == "SELECT 'it''s ?' WHERE id = $1"

  test "JSONB ?| operator preserved":
    check sqlParams("SELECT * FROM t WHERE col ?| array[?]") ==
      "SELECT * FROM t WHERE col ?| array[$1]"

  test "JSONB ?& operator preserved":
    check sqlParams("SELECT * FROM t WHERE col ?& array[?]") ==
      "SELECT * FROM t WHERE col ?& array[$1]"

  test "no placeholders":
    check sqlParams("SELECT 1") == "SELECT 1"

  test "empty string":
    check sqlParams("") == ""

  test "multiple placeholders numbered sequentially":
    check sqlParams("INSERT INTO t VALUES (?, ?, ?)") ==
      "INSERT INTO t VALUES ($1, $2, $3)"

  test "? inside double-quoted identifier preserved":
    check sqlParams("""SELECT "col?" FROM t WHERE id = ?""") ==
      """SELECT "col?" FROM t WHERE id = $1"""

  test "escaped double quote inside identifier":
    check sqlParams("""SELECT "col""?" FROM t WHERE id = ?""") ==
      """SELECT "col""?" FROM t WHERE id = $1"""

  test "? inside E-string preserved":
    check sqlParams("SELECT E'hello\\' ?' WHERE id = ?") ==
      "SELECT E'hello\\' ?' WHERE id = $1"

  test "E-string backslash escape":
    check sqlParams("SELECT E'a\\'b' WHERE id = ?") == "SELECT E'a\\'b' WHERE id = $1"

  test "E-string not triggered by identifier":
    # CRATE'?' — the E in CRATE is part of an identifier, not E-string prefix
    check sqlParams("SELECT CRATE'?' WHERE id = ?") == "SELECT CRATE'?' WHERE id = $1"

  test "? inside dollar-quoted string preserved":
    check sqlParams("SELECT $$hello ? world$$ WHERE id = ?") ==
      "SELECT $$hello ? world$$ WHERE id = $1"

  test "? inside tagged dollar-quoted string preserved":
    check sqlParams("SELECT $fn$hello ? world$fn$ WHERE id = ?") ==
      "SELECT $fn$hello ? world$fn$ WHERE id = $1"

  test "dollar sign not starting a dollar-quote":
    check sqlParams("SELECT $1 WHERE id = ?") == "SELECT $1 WHERE id = $1"

suite "sql macro":
  test "basic parameter extraction":
    let x = 42'i32
    let sq = sql"SELECT * FROM t WHERE id = {x}"
    check sq.query == "SELECT * FROM t WHERE id = $1"
    check sq.params.len == 1

  test "multiple parameters":
    let a = 1'i32
    let b = "hello"
    let sq = sql"SELECT * FROM t WHERE id = {a} AND name = {b}"
    check sq.query == "SELECT * FROM t WHERE id = $1 AND name = $2"
    check sq.params.len == 2

  test "no parameters":
    let sq = sql"SELECT 1"
    check sq.query == "SELECT 1"
    check sq.params.len == 0

  test "{{ and }} escape to literal braces":
    let sq = sql"SELECT {{}} AS col"
    check sq.query == "SELECT {} AS col"
    check sq.params.len == 0

  test "{{ and }} inside single-quoted string preserved":
    let sq = sql"SELECT '{{}}'::jsonb"
    check sq.query == "SELECT '{{}}'::jsonb"
    check sq.params.len == 0

  test "placeholder inside single-quoted string preserved":
    let sq = sql"SELECT 'hello {world}' AS col"
    check sq.query == "SELECT 'hello {world}' AS col"
    check sq.params.len == 0

  test "expression parameter":
    let x = 10'i32
    let sq = sql"SELECT * FROM t WHERE id = {x + 5'i32}"
    check sq.query == "SELECT * FROM t WHERE id = $1"
    check sq.params.len == 1

  test "field access parameter":
    type Obj = object
      age: int32

    let o = Obj(age: 25)
    let sq = sql"SELECT * FROM t WHERE age > {o.age}"
    check sq.query == "SELECT * FROM t WHERE age > $1"
    check sq.params.len == 1

  test "param value roundtrip int32":
    let v = 42'i32
    let sq = sql"SELECT {v}"
    check sq.query == "SELECT $1"
    check sq.params[0].oid == OidInt4
    check sq.params[0].value.isSome

  test "param value roundtrip string":
    let v = "hello"
    let sq = sql"SELECT {v}"
    check sq.query == "SELECT $1"
    check sq.params[0].oid == OidText
    check sq.params[0].value.isSome

  test "placeholder inside double-quoted identifier preserved":
    let x = 1'i32
    let sq = sql"""SELECT "{col}" FROM t WHERE id = {x}"""
    check sq.query == """SELECT "{col}" FROM t WHERE id = $1"""
    check sq.params.len == 1

  test "placeholder inside E-string preserved":
    let x = 1'i32
    let sq = sql"SELECT E'hello\' {world}' WHERE id = {x}"
    check sq.query == "SELECT E'hello\\' {world}' WHERE id = $1"
    check sq.params.len == 1

  test "placeholder inside dollar-quoted string preserved":
    let x = 1'i32
    let sq = sql"SELECT $${hello}$$ WHERE id = {x}"
    check sq.query == "SELECT $${hello}$$ WHERE id = $1"
    check sq.params.len == 1

  test "placeholder inside tagged dollar-quoted string preserved":
    let x = 1'i32
    let sq = sql"SELECT $fn${hello}$fn$ WHERE id = {x}"
    check sq.query == "SELECT $fn${hello}$fn$ WHERE id = $1"
    check sq.params.len == 1
