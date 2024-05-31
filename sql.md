# Catalogs

## Session

Default catalogs on session create:

- "system"
  - Contains built in functions.
  - Contains built in vies and tables (**unimplemented**)
  - Read only
- "temp"
  - Contains temp objects that last the lifetime of a session.

## Remote execution context

Catalogs on the "remote" side during distributed or hybrid execution.

**unimplemented**

- "system"

---

# Resolving objects

---

# Statements

## `ALTER TABLE`

**unimplemented**

## `ALTER VIEW`

**unimplemented**

## `COPY`

**unimplemented**

## `CREATE SCHEMA`

**unimplemented**

## `CREATE TABLE`

**unimplemented**

## `CREATE VIEW`

**unimplemented**

## `DELETE`

**unimplemented**

## `DROP SCHEMA`

**unimplemented**

## `DROP TABLE`

**unimplemented**

## `DROP VIEW`

**unimplemented**

## `INSERT`

**unimplemented**

## `SELECT`

**unimplemented**

## `SET` / `RESET`

**unimplemented**

## `UPDATE`

**unimplemented**

## `USE`

**unimplemented**

---

# Functions

---

# Variables

| Name                            |
|---------------------------------|
| debug_string_var                |
| application_name                |
| debug_error_on_nested_loop_join |
| partitions                      |
| batch_size                      |

---

# Data types

| SQL data type               | Execution data type |
|-----------------------------|---------------------|
| `VARCHAR`, `TEXT`, `STRING` | Utf8                |
| `SMALLINT`, `INT2`          | Int16               |
| `INTEGER`, `INT`, `INT4`    | Int32               |
| `BIGINT`, `INT8`            | Int64               |
| `REAL`, `FLOAT`, `FLOAT4`   | Float32             |
| `DOUBLE`, `FLOAT8`          | Float64             |
| `BOOL`, `BOOLEAN`           | Bool                |

---

# Identifiers

Identifiers are case-insenstive unless quoted with `"`.


