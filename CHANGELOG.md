# Changelog

## v0.1.0 (2026-03-21)

Initial release of Dux — a DuckDB-native dataframe library for Elixir.

### Core Engine

- `%Dux{}` struct — the module IS the dataframe
- Lazy pipelines: operations accumulate, `compute/1` executes
- CTE-based SQL compilation via `Dux.QueryBuilder`
- DuckDB NIF layer via Rustler (connection, query, Arrow IPC, temp tables)
- Connection GenServer with mutex serialization
- Perseus-style optimization: `Arc::strong_count()` check for in-place mutation

### Verb API

- `filter/2`, `mutate/2`, `summarise/2` — macros with Elixir expression syntax
- `select/2`, `discard/2`, `rename/2` — column operations
- `sort_by/2`, `head/2`, `slice/3`, `distinct/1`, `drop_nil/2` — row operations
- `group_by/2`, `ungroup/1` — grouping
- `join/3` — inner, left, right, cross, anti, semi joins
- `concat_rows/1` — UNION ALL
- `compute/1`, `collect/1`, `to_columns/1`, `n_rows/1` — materialization
- `sql_preview/1` — debugging

### Query Compiler

- `Dux.Query` — macro captures Elixir AST → DuckDB SQL
- `^pin` interpolation → parameter bindings (SQL injection safe)
- `col("name")` for columns with special characters
- Function calls (sum, avg, count, upper, etc.) pass through to DuckDB

### IO

- `from_csv/2`, `from_parquet/2`, `from_ndjson/2` — lazy sources
- `to_csv/3`, `to_parquet/3`, `to_ndjson/3` — sinks via COPY
- DuckDB extensions for S3, HTTP, Postgres, MySQL, SQLite

### Distributed (spark-killer)

- `Dux.Remote.Worker` — DuckDB per BEAM node, `:pg` discovery
- `Dux.Remote.Coordinator` — partition, fan-out, merge
- `Dux.Remote.Broadcast` — star-schema broadcast joins
- `Dux.Remote` — distributed GC for NIF resources across nodes
- GC sentinel NIF with `OwnedEnv` + `SavedTerm`
- Arrow IPC over BEAM distribution

### Graph Analytics

- `Dux.Graph` — graph as two dataframes (vertices + edges)
- `pagerank/2` — iterative join-aggregate
- `shortest_paths/2` — recursive CTE
- `connected_components/1` — label propagation
- `triangle_count/1` — triple self-join
- `out_degree/1`, `in_degree/1`, `degree/1`

### Nx Interop

- `Nx.LazyContainer` protocol implementation
- `Dux.to_tensor/2` for explicit column → tensor conversion
- Supports s8-s64, u8-u64, f32-f64, boolean, decimal types

### Security

- SQL identifier escaping (double-quote doubling)
- Parameter bindings for all `^pin` interpolations
- CSV delimiter escaping

### Infrastructure

- Rustler 0.37 + RustlerPrecompiled
- Precompiled binaries for 6 targets
- GitHub Actions CI (OTP 27+28, Elixir 1.18+1.19)
- Benchee benchmark suite
- Credo strict compliance
- Dual Apache 2.0 + MIT license
