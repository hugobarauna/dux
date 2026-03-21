defmodule Dux.Native do
  @moduledoc false

  use Rustler,
    otp_app: :dux,
    crate: "dux"

  # Database lifecycle
  def db_open(), do: :erlang.nif_error(:nif_not_loaded)
  def db_open_path(_path), do: :erlang.nif_error(:nif_not_loaded)

  # Query execution — the only path into DuckDB
  def db_execute_sql(_conn, _sql, _params), do: :erlang.nif_error(:nif_not_loaded)
  def db_query_arrow(_conn, _sql, _params), do: :erlang.nif_error(:nif_not_loaded)
end
