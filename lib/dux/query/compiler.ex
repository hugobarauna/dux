defmodule Dux.Query.Compiler do
  @moduledoc false

  # Compiles Dux query AST nodes into {sql_string, params} tuples.
  # The AST is produced by Dux.Query macros at compile time.
  # Pins become $N parameter bindings — values never appear in SQL strings.

  @doc """
  Compile a Dux query AST node into `{sql_string, params_list}`.

  The `pins` list contains the runtime values for `^` interpolations,
  in the order they were encountered during macro expansion.
  """
  def to_sql(ast, pins) do
    {sql, params, _idx} = compile(ast, pins, 0)
    {sql, params}
  end

  # --- Literals ---

  defp compile({:lit, nil}, _pins, idx) do
    {"NULL", [], idx}
  end

  defp compile({:lit, value}, _pins, idx) when is_integer(value) do
    {Integer.to_string(value), [], idx}
  end

  defp compile({:lit, value}, _pins, idx) when is_float(value) do
    {Float.to_string(value), [], idx}
  end

  defp compile({:lit, value}, _pins, idx) when is_binary(value) do
    escaped = String.replace(value, "'", "''")
    {"'#{escaped}'", [], idx}
  end

  defp compile({:lit, true}, _pins, idx), do: {"true", [], idx}
  defp compile({:lit, false}, _pins, idx), do: {"false", [], idx}

  # --- Column references ---

  defp compile({:column, name}, _pins, idx) do
    {quote_ident(name), [], idx}
  end

  defp compile({:dynamic_column, pin_idx}, pins, idx) do
    name = Enum.at(pins, pin_idx)
    {quote_ident(name), [], idx}
  end

  # --- Pin (interpolated value) ---

  defp compile({:pin, pin_idx}, pins, idx) do
    value = Enum.at(pins, pin_idx)
    {"$#{idx + 1}", [value], idx + 1}
  end

  # --- Binary operators ---

  @binary_ops %{
    eq: "=",
    neq: "!=",
    gt: ">",
    gte: ">=",
    lt: "<",
    lte: "<=",
    add: "+",
    sub: "-",
    mul: "*",
    div: "/",
    and: "AND",
    or: "OR"
  }

  for {op, sql_op} <- @binary_ops do
    defp compile({unquote(op), left, right}, pins, idx) do
      {l_sql, l_params, idx} = compile(left, pins, idx)
      {r_sql, r_params, idx} = compile(right, pins, idx)
      {"(#{l_sql} #{unquote(sql_op)} #{r_sql})", l_params ++ r_params, idx}
    end
  end

  # --- Unary operators ---

  defp compile({:not, expr}, pins, idx) do
    {sql, params, idx} = compile(expr, pins, idx)
    {"(NOT #{sql})", params, idx}
  end

  defp compile({:negate, expr}, pins, idx) do
    {sql, params, idx} = compile(expr, pins, idx)
    {"(- #{sql})", params, idx}
  end

  # --- String concatenation ---

  defp compile({:concat, left, right}, pins, idx) do
    {l_sql, l_params, idx} = compile(left, pins, idx)
    {r_sql, r_params, idx} = compile(right, pins, idx)
    {"(#{l_sql} || #{r_sql})", l_params ++ r_params, idx}
  end

  # --- Function calls (aggregations, DuckDB functions) ---

  @known_aggregations ~w(sum avg mean min max count count_distinct std variance)a
  @known_functions ~w(abs round ceil floor length lower upper trim
                      cast coalesce nullif greatest least
                      year month day hour minute second
                      date_trunc date_part epoch
                      regexp_matches regexp_replace regexp_extract
                      string_split list_value list_sort)a

  defp compile({:call, :mean, args}, pins, idx) do
    # mean is AVG in SQL
    compile({:call, :avg, args}, pins, idx)
  end

  defp compile({:call, :std, args}, pins, idx) do
    compile({:call, :stddev_samp, args}, pins, idx)
  end

  defp compile({:call, :count_distinct, [arg]}, pins, idx) do
    {arg_sql, arg_params, idx} = compile(arg, pins, idx)
    {"COUNT(DISTINCT #{arg_sql})", arg_params, idx}
  end

  defp compile({:call, func, args}, pins, idx)
       when func in @known_aggregations or func in @known_functions or true do
    # Generic function call — pass through to DuckDB
    sql_name = func |> to_string() |> String.upcase()

    {arg_sqls, all_params, idx} =
      Enum.reduce(args, {[], [], idx}, fn arg, {sqls, params, idx} ->
        {sql, new_params, idx} = compile(arg, pins, idx)
        {sqls ++ [sql], params ++ new_params, idx}
      end)

    {"#{sql_name}(#{Enum.join(arg_sqls, ", ")})", all_params, idx}
  end

  # --- Sort direction markers ---

  defp compile({:asc, expr}, pins, idx) do
    {sql, params, idx} = compile(expr, pins, idx)
    {"#{sql} ASC", params, idx}
  end

  defp compile({:desc, expr}, pins, idx) do
    {sql, params, idx} = compile(expr, pins, idx)
    {"#{sql} DESC", params, idx}
  end

  # --- Helpers ---

  defp quote_ident(name) do
    escaped = String.replace(name, ~s("), ~s(""))
    ~s("#{escaped}")
  end
end
