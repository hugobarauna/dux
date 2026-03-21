defmodule Dux.QueryBuilder do
  @moduledoc false

  # Walks a %Dux{} ops list and emits CTE-based SQL.
  # Each operation becomes a CTE: __s0, __s1, __s2, ...
  # DuckDB handles all pushdown optimization across the CTEs.
end
