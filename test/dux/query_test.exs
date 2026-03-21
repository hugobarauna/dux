defmodule Dux.QueryTest do
  use ExUnit.Case, async: false
  require Dux.Q

  # ---------------------------------------------------------------------------
  # Happy path — compiler output
  # ---------------------------------------------------------------------------

  describe "Dux.Q.q/1 compilation" do
    test "column reference" do
      assert {"\"x\"", []} = Dux.Q.q(x)
    end

    test "literal integer" do
      assert {"42", []} = Dux.Q.q(42)
    end

    test "literal float" do
      assert {sql, []} = Dux.Q.q(3.14)
      assert sql =~ "3.14"
    end

    test "literal string" do
      assert {"'hello'", []} = Dux.Q.q("hello")
    end

    test "literal boolean" do
      assert {"true", []} = Dux.Q.q(true)
      assert {"false", []} = Dux.Q.q(false)
    end

    test "comparison operators" do
      assert {"(\"x\" > 10)", []} = Dux.Q.q(x > 10)
      assert {"(\"x\" >= 10)", []} = Dux.Q.q(x >= 10)
      assert {"(\"x\" < 10)", []} = Dux.Q.q(x < 10)
      assert {"(\"x\" <= 10)", []} = Dux.Q.q(x <= 10)
      assert {"(\"x\" = 10)", []} = Dux.Q.q(x == 10)
      assert {"(\"x\" != 10)", []} = Dux.Q.q(x != 10)
    end

    test "arithmetic operators" do
      assert {"(\"x\" + \"y\")", []} = Dux.Q.q(x + y)
      assert {"(\"x\" - \"y\")", []} = Dux.Q.q(x - y)
      assert {"(\"x\" * \"y\")", []} = Dux.Q.q(x * y)
      assert {"(\"x\" / \"y\")", []} = Dux.Q.q(x / y)
    end

    test "logical operators" do
      assert {"(\"x\" AND \"y\")", []} = Dux.Q.q(x and y)
      assert {"(\"x\" OR \"y\")", []} = Dux.Q.q(x or y)
      assert {"(NOT \"x\")", []} = Dux.Q.q(not x)
    end

    test "compound expression" do
      {sql, []} = Dux.Q.q(x > 10 and y < 20)
      assert sql == "((\"x\" > 10) AND (\"y\" < 20))"
    end

    test "nested arithmetic" do
      {sql, []} = Dux.Q.q((x + y) * z)
      assert sql == "((\"x\" + \"y\") * \"z\")"
    end

    test "function calls" do
      assert {"SUM(\"x\")", []} = Dux.Q.q(sum(x))
      assert {"AVG(\"x\")", []} = Dux.Q.q(avg(x))
      assert {"AVG(\"x\")", []} = Dux.Q.q(mean(x))
      assert {"COUNT(\"x\")", []} = Dux.Q.q(count(x))
      assert {"MIN(\"x\")", []} = Dux.Q.q(min(x))
      assert {"MAX(\"x\")", []} = Dux.Q.q(max(x))
    end

    test "count_distinct" do
      assert {"COUNT(DISTINCT \"x\")", []} = Dux.Q.q(count_distinct(x))
    end

    test "nested function in expression" do
      {sql, []} = Dux.Q.q(sum(x) / count(x))
      assert sql == "(SUM(\"x\") / COUNT(\"x\"))"
    end

    test "DuckDB functions pass through" do
      assert {"UPPER(\"name\")", []} = Dux.Q.q(upper(name))
      assert {"LOWER(\"name\")", []} = Dux.Q.q(lower(name))
      assert {"ABS(\"x\")", []} = Dux.Q.q(abs(x))
      assert {"ROUND(\"x\", 2)", []} = Dux.Q.q(round(x, 2))
      assert {"COALESCE(\"x\", 0)", []} = Dux.Q.q(coalesce(x, 0))
    end

    test "col() for unusual column names" do
      assert {"\"col with spaces\"", []} = Dux.Q.q(col("col with spaces"))
    end
  end

  # ---------------------------------------------------------------------------
  # Pin interpolation
  # ---------------------------------------------------------------------------

  describe "pin interpolation" do
    test "integer pin" do
      min_val = 10
      {sql, params} = Dux.Q.q(x > ^min_val)
      assert sql == "(\"x\" > $1)"
      assert params == [10]
    end

    test "string pin" do
      name = "Alice"
      {sql, params} = Dux.Q.q(x == ^name)
      assert sql == "(\"x\" = $1)"
      assert params == ["Alice"]
    end

    test "multiple pins" do
      lo = 5
      hi = 15
      {sql, params} = Dux.Q.q(x > ^lo and x < ^hi)
      assert sql == "((\"x\" > $1) AND (\"x\" < $2))"
      assert params == [5, 15]
    end

    test "pin with expression" do
      base = 10
      {sql, params} = Dux.Q.q(x > ^(base * 2))
      assert sql == "(\"x\" > $1)"
      assert params == [20]
    end

    test "dynamic column name with col(^name)" do
      col_name = "my column"
      {sql, []} = Dux.Q.q(col(^col_name))
      assert sql == "\"my column\""
    end
  end

  # ---------------------------------------------------------------------------
  # Integration with Dux verbs
  # ---------------------------------------------------------------------------

  describe "filter with q/1" do
    test "basic filter" do
      result =
        Dux.from_query("SELECT * FROM range(1, 6) t(x)")
        |> Dux.filter(Dux.Q.q(x > 3))
        |> Dux.to_columns()

      assert result == %{"x" => [4, 5]}
    end

    test "filter with pin" do
      threshold = 3

      result =
        Dux.from_query("SELECT * FROM range(1, 6) t(x)")
        |> Dux.filter(Dux.Q.q(x > ^threshold))
        |> Dux.to_columns()

      assert result == %{"x" => [4, 5]}
    end

    test "filter with compound expression" do
      result =
        Dux.from_query("SELECT * FROM range(1, 21) t(x)")
        |> Dux.filter(Dux.Q.q(x > 5 and x < 10))
        |> Dux.to_columns()

      assert result == %{"x" => [6, 7, 8, 9]}
    end

    test "filter with string pin" do
      target = "Alice"

      result =
        Dux.from_list([
          %{"name" => "Alice", "age" => 30},
          %{"name" => "Bob", "age" => 25}
        ])
        |> Dux.filter(Dux.Q.q(name == ^target))
        |> Dux.to_columns()

      assert result["name"] == ["Alice"]
    end
  end

  describe "mutate with q/1" do
    test "basic mutate" do
      result =
        Dux.from_query("SELECT 10 AS price, 5 AS qty")
        |> Dux.mutate(revenue: Dux.Q.q(price * qty))
        |> Dux.collect()

      assert [%{"revenue" => 50}] = result
    end

    test "mutate with pin" do
      tax_rate = 0.1

      result =
        Dux.from_query("SELECT 100 AS price")
        |> Dux.mutate(tax: Dux.Q.q(price * ^tax_rate))
        |> Dux.collect()

      row = hd(result)
      assert_in_delta row["tax"], 10.0, 0.01
    end

    test "mutate with function call" do
      result =
        Dux.from_list([%{"name" => "alice"}])
        |> Dux.mutate(upper_name: Dux.Q.q(upper(name)))
        |> Dux.to_columns()

      assert result["upper_name"] == ["ALICE"]
    end
  end

  describe "summarise with q/1" do
    test "basic aggregation" do
      result =
        Dux.from_list([%{"g" => "a", "v" => 1}, %{"g" => "a", "v" => 2}, %{"g" => "b", "v" => 3}])
        |> Dux.group_by(:g)
        |> Dux.summarise(total: Dux.Q.q(sum(v)))
        |> Dux.sort_by(:g)
        |> Dux.to_columns()

      assert result["g"] == ["a", "b"]
      assert result["total"] == [3, 3]
    end

    test "multiple aggregations" do
      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.summarise(
          total: Dux.Q.q(sum(x)),
          average: Dux.Q.q(avg(x)),
          n: Dux.Q.q(count(x))
        )
        |> Dux.collect()

      row = hd(result)
      assert row["total"] == 55
      assert_in_delta row["average"], 5.5, 0.01
      assert row["n"] == 10
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "query with unknown column produces DuckDB error" do
      assert_raise ArgumentError, ~r/DuckDB query failed/, fn ->
        Dux.from_query("SELECT 1 AS x")
        |> Dux.filter(Dux.Q.q(nonexistent_column > 0))
        |> Dux.compute()
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "SQL injection via pin is prevented" do
      # The malicious value should be treated as a literal, not SQL
      malicious = "'; DROP TABLE users; --"

      result =
        Dux.from_list([%{"name" => "safe"}, %{"name" => "'; DROP TABLE users; --"}])
        |> Dux.filter(Dux.Q.q(name == ^malicious))
        |> Dux.to_columns()

      assert result["name"] == ["'; DROP TABLE users; --"]
    end

    test "deeply nested expression compiles" do
      # ((((x + 1) + 1) + 1) ... + 1) — 20 levels deep
      {sql, []} = Dux.Q.q(x + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1)
      assert sql =~ "\"x\""
      assert sql =~ "+"
    end

    test "many pins" do
      a = 1
      b = 2
      c = 3
      d = 4
      e = 5
      {sql, params} = Dux.Q.q((x > ^a and x < ^b) or (y == ^c and z != ^d) or w <= ^e)
      assert length(params) == 5
      assert sql =~ "$1"
      assert sql =~ "$5"
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "expression with mixed types compiles" do
      tax = 0.08

      result =
        Dux.from_list([%{"price" => 100, "name" => "widget"}])
        |> Dux.mutate(
          with_tax: Dux.Q.q(price * (1 + ^tax)),
          label: Dux.Q.q(upper(name))
        )
        |> Dux.collect()

      row = hd(result)
      assert_in_delta row["with_tax"], 108.0, 0.01
      assert row["label"] == "WIDGET"
    end

    test "chained filter with q then string" do
      result =
        Dux.from_query("SELECT * FROM range(1, 21) t(x)")
        |> Dux.filter(Dux.Q.q(x > 5))
        |> Dux.filter("x < 15")
        |> Dux.filter(Dux.Q.q(x != 10))
        |> Dux.to_columns()

      expected = Enum.to_list(6..14) -- [10]
      assert result["x"] == expected
    end
  end
end
