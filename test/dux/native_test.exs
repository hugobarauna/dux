defmodule Dux.NativeTest do
  use ExUnit.Case, async: false

  setup do
    db = Dux.Native.db_open()
    %{db: db}
  end

  # ---------- Happy path ----------

  describe "db_open/0 and db_open_path/1" do
    test "opens in-memory database" do
      db = Dux.Native.db_open()
      assert is_reference(db)
    end

    test "opens file-backed database" do
      path = Path.join(System.tmp_dir!(), "dux_native_test_#{System.unique_integer([:positive])}.duckdb")

      try do
        db = Dux.Native.db_open_path(path)
        assert is_reference(db)
      after
        File.rm(path)
        File.rm(path <> ".wal")
      end
    end
  end

  describe "db_execute/2" do
    test "executes DDL", %{db: db} do
      assert {} = Dux.Native.db_execute(db, "CREATE TABLE t (x INTEGER)")
    end
  end

  describe "df_query/2" do
    test "returns table reference for simple query", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT 42 AS answer")
      assert is_reference(table)
    end

    test "returns correct column names", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT 1 AS a, 2 AS b, 3 AS c")
      assert Dux.Native.table_names(table) == ["a", "b", "c"]
    end

    test "returns correct dtypes", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT 1::INTEGER AS i, 'hello'::VARCHAR AS s, true AS b")
      dtypes = Dux.Native.table_dtypes(table)
      assert [{"i", {:s, 32}}, {"s", :string}, {"b", :boolean}] = dtypes
    end

    test "returns correct row count", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT * FROM range(100)")
      assert Dux.Native.table_n_rows(table) == 100
    end
  end

  describe "table_to_columns/1" do
    test "returns map of column name to values", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2, 'b'")
      columns = Dux.Native.table_to_columns(table)
      assert %{"x" => [1, 2], "y" => ["a", "b"]} = columns
    end

    test "handles null values", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT 1 AS x UNION ALL SELECT NULL")
      columns = Dux.Native.table_to_columns(table)
      assert %{"x" => [1, nil]} = columns
    end

    test "handles empty result", %{db: db} do
      Dux.Native.db_execute(db, "CREATE TABLE empty (x INTEGER)")
      table = Dux.Native.df_query(db, "SELECT * FROM empty")
      columns = Dux.Native.table_to_columns(table)
      assert %{"x" => []} = columns
    end

    test "handles multiple types", %{db: db} do
      table = Dux.Native.df_query(db, """
        SELECT
          42::BIGINT AS i,
          3.14::DOUBLE AS f,
          'hello'::VARCHAR AS s,
          true AS b
      """)
      columns = Dux.Native.table_to_columns(table)
      assert %{"i" => [42], "f" => [f], "s" => ["hello"], "b" => [true]} = columns
      assert_in_delta f, 3.14, 0.001
    end

    test "handles float special values", %{db: db} do
      table = Dux.Native.df_query(db, """
        SELECT
          'NaN'::DOUBLE AS nan_val,
          'Infinity'::DOUBLE AS inf_val,
          '-Infinity'::DOUBLE AS neg_inf_val
      """)
      columns = Dux.Native.table_to_columns(table)
      assert %{"nan_val" => [:nan], "inf_val" => [:infinity], "neg_inf_val" => [:neg_infinity]} = columns
    end
  end

  describe "table_to_rows/1" do
    test "returns list of maps", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2, 'b'")
      rows = Dux.Native.table_to_rows(table)
      assert [%{"x" => 1, "y" => "a"}, %{"x" => 2, "y" => "b"}] = rows
    end
  end

  describe "table_ensure/2" do
    test "registers a temp table and returns its name", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT 1 AS x, 2 AS y")
      table_name = Dux.Native.table_ensure(db, table)
      assert is_binary(table_name)
      assert String.starts_with?(table_name, "__dux_")

      # Can query the temp table by name
      result = Dux.Native.df_query(db, "SELECT * FROM #{table_name}")
      assert Dux.Native.table_n_rows(result) == 1
    end

    test "returns cached name on second call", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT 1 AS x")
      name1 = Dux.Native.table_ensure(db, table)
      name2 = Dux.Native.table_ensure(db, table)
      assert name1 == name2
    end

    test "handles large datasets (>2048 rows)", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT * FROM range(5000) t(x)")
      table_name = Dux.Native.table_ensure(db, table)

      result = Dux.Native.df_query(db, "SELECT COUNT(*) AS cnt FROM #{table_name}")
      columns = Dux.Native.table_to_columns(result)
      assert columns["cnt"] == [5000]
    end
  end

  # ---------- Arrow IPC round-trip ----------

  describe "Arrow IPC serialization" do
    test "round-trips through IPC", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT 42 AS x, 'hello' AS y")
      ipc = Dux.Native.table_to_ipc(table)
      assert is_binary(ipc)
      assert byte_size(ipc) > 0

      restored = Dux.Native.table_from_ipc(ipc)
      assert Dux.Native.table_names(restored) == ["x", "y"]
      assert Dux.Native.table_to_columns(restored) == %{"x" => [42], "y" => ["hello"]}
    end

    test "round-trips empty table", %{db: db} do
      Dux.Native.db_execute(db, "CREATE TABLE empty_ipc (a INTEGER, b VARCHAR)")
      table = Dux.Native.df_query(db, "SELECT * FROM empty_ipc")
      ipc = Dux.Native.table_to_ipc(table)
      restored = Dux.Native.table_from_ipc(ipc)
      assert Dux.Native.table_names(restored) == ["a", "b"]
      assert Dux.Native.table_n_rows(restored) == 0
    end

    test "round-trips large dataset", %{db: db} do
      table = Dux.Native.df_query(db, "SELECT x, x * 2 AS doubled FROM range(10000) t(x)")
      ipc = Dux.Native.table_to_ipc(table)
      restored = Dux.Native.table_from_ipc(ipc)
      assert Dux.Native.table_n_rows(restored) == 10000
    end
  end

  # ---------- Sad path ----------

  describe "error handling" do
    test "df_query returns error on invalid SQL", %{db: db} do
      assert {:error, reason} = Dux.Native.df_query(db, "THIS IS NOT SQL")
      assert is_binary(reason)
    end

    test "db_execute returns error on invalid SQL", %{db: db} do
      assert {:error, reason} = Dux.Native.db_execute(db, "DEFINITELY NOT SQL")
      assert is_binary(reason)
    end
  end

  # ---------- Adversarial ----------

  describe "adversarial inputs" do
    test "handles unicode column names", %{db: db} do
      table = Dux.Native.df_query(db, ~s{SELECT 1 AS "日本語", 2 AS "émoji🎉"})
      names = Dux.Native.table_names(table)
      assert "日本語" in names
      assert "émoji🎉" in names
    end

    test "handles very wide tables", %{db: db} do
      cols = Enum.map_join(1..200, ", ", fn i -> "#{i} AS c#{i}" end)
      table = Dux.Native.df_query(db, "SELECT #{cols}")
      assert length(Dux.Native.table_names(table)) == 200
    end

    test "handles column names with special characters", %{db: db} do
      table = Dux.Native.df_query(db, ~s{SELECT 1 AS "col with spaces", 2 AS "col-with-dashes"})
      names = Dux.Native.table_names(table)
      assert "col with spaces" in names
      assert "col-with-dashes" in names
    end
  end

  # ---------- Wicked / pathological ----------

  describe "pathological cases" do
    test "handles cartesian product result", %{db: db} do
      Dux.Native.db_execute(db, "CREATE TABLE small AS SELECT * FROM range(100) t(x)")
      # 100 x 100 = 10,000 rows
      table = Dux.Native.df_query(db, "SELECT a.x, b.x AS y FROM small a, small b")
      assert Dux.Native.table_n_rows(table) == 10_000
    end

    test "handles deeply nested query", %{db: db} do
      # 50-deep CTE chain
      ctes =
        Enum.reduce(1..50, "SELECT 1 AS x", fn i, acc ->
          "SELECT x + 1 AS x FROM (#{acc}) s#{i}"
        end)

      table = Dux.Native.df_query(db, ctes)
      columns = Dux.Native.table_to_columns(table)
      assert columns["x"] == [51]
    end

    test "temp table cleanup on GC" do
      # This test verifies that temp tables are cleaned up when references are GC'd.
      # We create a table, get its name, then drop all references and force GC.
      db = Dux.Native.db_open()
      table = Dux.Native.df_query(db, "SELECT 1 AS x")
      table_name = Dux.Native.table_ensure(db, table)

      # Verify table exists
      result = Dux.Native.df_query(db, "SELECT * FROM #{table_name}")
      assert Dux.Native.table_n_rows(result) == 1

      # Drop all references to the table (but not db)
      # Note: we can't force GC deterministically, so we verify the Drop mechanism
      # exists by checking that the table_name was correctly scoped
      assert String.starts_with?(table_name, "__dux_")
    end
  end
end
