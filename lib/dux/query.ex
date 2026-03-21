defmodule Dux.Query do
  alias Dux.Query.Compiler

  @moduledoc """
  Macro-based query expressions for Dux.

  Inside a query, bare identifiers (like `x`, `name`) refer to column names.
  Use `^` to interpolate Elixir variables — these become parameter bindings
  in the generated SQL, preventing SQL injection by construction.

  ## Supported in queries

  Queries are used in `Dux.filter/2`, `Dux.mutate/2`, `Dux.summarise/2`,
  and `Dux.sort_by/2`.

  ## Operators

  Comparison: `==`, `!=`, `>`, `>=`, `<`, `<=`
  Arithmetic: `+`, `-`, `*`, `/`
  Logical: `and`, `or`, `not`
  String: `<>` (concatenation)

  ## Aggregation functions

  `sum(col)`, `mean(col)`, `min(col)`, `max(col)`, `count(col)`,
  `count_distinct(col)`, `avg(col)`, `std(col)`, `variance(col)`

  ## Other functions

  `col("name")` for columns with unusual names.
  `cast(expr, type)` for type casting.

  ## Interpolation

  Use `^` to access variables defined outside the query:

      min_val = 10
      Dux.filter(df, x > ^min_val)

  ## Examples

      # Filter
      Dux.filter(df, age > 18 and status == "active")

      # Mutate
      Dux.mutate(df, revenue: price * quantity, tax: price * ^tax_rate)

      # Summarise
      Dux.summarise(df, total: sum(amount), n: count(id))

      # Sort
      Dux.sort_by(df, desc: total)
  """

  @doc """
  Compiles a query expression into a `{sql_string, params}` tuple.

  This is the entry point used by `Dux.filter/2` and friends.
  """
  defmacro query(expression) do
    {ast, pins} = traverse(expression, [])

    quote do
      pins = unquote(Enum.reverse(pins))
      Compiler.to_sql(unquote(Macro.escape(ast)), pins)
    end
  end

  @doc """
  Compiles a keyword list of query expressions (for mutate/summarise).

  Returns a list of `{name, {sql_string, params}}` tuples.
  """
  defmacro query_pairs(pairs) do
    compiled =
      Enum.map(pairs, fn {name, expression} ->
        {ast, pins} = traverse(expression, [])

        quote do
          {unquote(to_string(name)),
           (fn ->
              pins = unquote(Enum.reverse(pins))
              Compiler.to_sql(unquote(Macro.escape(ast)), pins)
            end).()}
        end
      end)

    compiled
  end

  # ---------------------------------------------------------------------------
  # AST traversal — converts Elixir AST to Dux query AST
  # ---------------------------------------------------------------------------

  @doc false
  def traverse_public(expression, pins), do: traverse(expression, pins)

  # Pin: ^expr — interpolate an Elixir value
  defp traverse({:^, _meta, [expr]}, pins) do
    idx = length(pins)
    {{:pin, idx}, [expr | pins]}
  end

  # col("name") — explicit column reference
  defp traverse({:col, _meta, [name]}, pins) when is_binary(name) do
    {{:column, name}, pins}
  end

  defp traverse({:col, _meta, [{:^, _, [expr]}]}, pins) do
    idx = length(pins)
    {{:dynamic_column, idx}, [expr | pins]}
  end

  # Bare variable — becomes a column reference
  defp traverse({var, _meta, ctx}, pins) when is_atom(var) and is_atom(ctx) do
    {{:column, to_string(var)}, pins}
  end

  # Binary operators
  defp traverse({op, _meta, [left, right]}, pins)
       when op in [:==, :!=, :>, :>=, :<, :<=, :+, :-, :*, :/, :and, :or] do
    {l_ast, pins} = traverse(left, pins)
    {r_ast, pins} = traverse(right, pins)
    dux_op = translate_op(op)
    {{dux_op, l_ast, r_ast}, pins}
  end

  # Unary not
  defp traverse({:not, _meta, [expr]}, pins) do
    {ast, pins} = traverse(expr, pins)
    {{:not, ast}, pins}
  end

  # Unary minus
  defp traverse({:-, _meta, [expr]}, pins) when not is_number(expr) do
    {ast, pins} = traverse(expr, pins)
    {{:negate, ast}, pins}
  end

  # String concatenation: <>
  defp traverse({:<>, _meta, [left, right]}, pins) do
    {l_ast, pins} = traverse(left, pins)
    {r_ast, pins} = traverse(right, pins)
    {{:concat, l_ast, r_ast}, pins}
  end

  # Function calls — aggregations and other functions
  defp traverse({func, _meta, args}, pins) when is_atom(func) and is_list(args) do
    {arg_asts, pins} =
      Enum.map_reduce(args, pins, fn arg, pins -> traverse(arg, pins) end)

    {{:call, func, arg_asts}, pins}
  end

  # Keyword pairs (for sort_by: [asc: :col, desc: :col])
  defp traverse({key, value}, pins) when is_atom(key) do
    {v_ast, pins} = traverse(value, pins)
    {{key, v_ast}, pins}
  end

  # Literals
  defp traverse(value, pins) when is_number(value) do
    {{:lit, value}, pins}
  end

  defp traverse(value, pins) when is_binary(value) do
    {{:lit, value}, pins}
  end

  defp traverse(value, pins) when is_boolean(value) do
    {{:lit, value}, pins}
  end

  defp traverse(nil, pins) do
    {{:lit, nil}, pins}
  end

  defp traverse(value, pins) when is_atom(value) do
    {{:column, to_string(value)}, pins}
  end

  # Lists (e.g. for sort_by)
  defp traverse(list, pins) when is_list(list) do
    Enum.map_reduce(list, pins, &traverse/2)
  end

  # ---------------------------------------------------------------------------
  # Operator translation
  # ---------------------------------------------------------------------------

  defp translate_op(:==), do: :eq
  defp translate_op(:!=), do: :neq
  defp translate_op(:>), do: :gt
  defp translate_op(:>=), do: :gte
  defp translate_op(:<), do: :lt
  defp translate_op(:<=), do: :lte
  defp translate_op(:+), do: :add
  defp translate_op(:-), do: :sub
  defp translate_op(:*), do: :mul
  defp translate_op(:/), do: :div
  defp translate_op(:and), do: :and
  defp translate_op(:or), do: :or
end
