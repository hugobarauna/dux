defmodule Dux.Q do
  alias Dux.Query.Compiler

  @moduledoc """
  Query expression helpers for use with Dux verbs.

  `require Dux.Q` to use `q/1` which compiles Elixir expressions to SQL:

      require Dux.Q

      Dux.from_query("SELECT * FROM range(1, 11) t(x)")
      |> Dux.filter(Dux.Q.q(x > 5))
      |> Dux.to_columns()
      # %{"x" => [6, 7, 8, 9, 10]}

  ## Interpolation with ^

      min_val = 3
      Dux.from_query("SELECT * FROM range(1, 11) t(x)")
      |> Dux.filter(Dux.Q.q(x > ^min_val))
      |> Dux.to_columns()
      # %{"x" => [4, 5, 6, 7, 8, 9, 10]}

  ## Mutate with expressions

      Dux.from_query("SELECT 10 AS price, 5 AS qty")
      |> Dux.mutate(revenue: Dux.Q.q(price * qty))
      |> Dux.collect()
      # [%{"price" => 10, "qty" => 5, "revenue" => 50}]

  ## Aggregations

      Dux.from_list([%{"x" => 1}, %{"x" => 2}, %{"x" => 3}])
      |> Dux.summarise(total: Dux.Q.q(sum(x)), n: Dux.Q.q(count(x)))
      |> Dux.collect()
  """

  @doc """
  Compile an Elixir expression to a `{sql, params}` tuple for use in Dux verbs.

  Bare identifiers become column references. `^` interpolates Elixir values
  as parameter bindings (SQL injection safe).

  ## Examples

      require Dux.Q

      Dux.Q.q(x > 10)           # {"(\\"x\\" > 10)", []}
      Dux.Q.q(x > ^min_val)     # {"(\\"x\\" > $1)", [min_val]}
      Dux.Q.q(sum(amount))      # {"SUM(\\"amount\\")", []}
      Dux.Q.q(price * qty)      # {"(\\"price\\" * \\"qty\\")", []}
  """
  defmacro q(expression) do
    {ast, pins} = Dux.Query.traverse_public(expression, [])

    quote do
      pins = unquote(Enum.reverse(pins))
      Compiler.to_sql(unquote(Macro.escape(ast)), pins)
    end
  end
end
