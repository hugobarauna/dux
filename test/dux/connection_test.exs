defmodule Dux.ConnectionTest do
  use ExUnit.Case, async: false

  setup do
    # Start a fresh connection for each test
    name = :"conn_#{System.unique_integer([:positive])}"
    {:ok, pid} = Dux.Connection.start_link(name: name)
    %{conn: name, pid: pid}
  end

  # ---------- Happy path ----------

  describe "start_link/1" do
    test "opens an in-memory database", %{conn: conn} do
      db = Dux.Connection.get_db(conn)
      assert is_reference(db)
    end

    test "opens a file-backed database" do
      path = Path.join(System.tmp_dir!(), "dux_test_#{System.unique_integer([:positive])}.duckdb")
      name = :"conn_file_#{System.unique_integer([:positive])}"

      try do
        {:ok, _pid} = Dux.Connection.start_link(name: name, path: path)
        db = Dux.Connection.get_db(name)
        assert is_reference(db)
        assert File.exists?(path)
      after
        File.rm(path)
        # DuckDB creates .wal files too
        File.rm(path <> ".wal")
      end
    end
  end

  describe "execute/2" do
    test "runs DDL statements", %{conn: conn} do
      assert :ok = Dux.Connection.execute("CREATE TABLE test (id INTEGER, name VARCHAR)", conn)
    end

    test "runs DML statements", %{conn: conn} do
      Dux.Connection.execute("CREATE TABLE test (id INTEGER)", conn)
      assert :ok = Dux.Connection.execute("INSERT INTO test VALUES (1), (2), (3)", conn)
    end
  end

  describe "query/2" do
    test "returns a table reference", %{conn: conn} do
      result = Dux.Connection.query("SELECT 1 AS x, 'hello' AS y", conn)
      assert is_reference(result)
    end

    test "handles empty results", %{conn: conn} do
      Dux.Connection.execute("CREATE TABLE empty (id INTEGER)", conn)
      result = Dux.Connection.query("SELECT * FROM empty", conn)
      assert is_reference(result)
      assert Dux.Native.table_n_rows(result) == 0
    end
  end

  describe "load_extension/2" do
    test "loads a built-in extension", %{conn: conn} do
      # json is a built-in autoloadable extension
      assert :ok = Dux.Connection.load_extension(:json, conn)
    end
  end

  # ---------- Sad path ----------

  describe "error handling" do
    test "execute returns error on invalid SQL", %{conn: conn} do
      assert {:error, _reason} = Dux.Connection.execute("NOT VALID SQL HERE!!!", conn)
    end

    test "query returns error on invalid SQL", %{conn: conn} do
      assert {:error, reason} = Dux.Connection.query("SELECT FROM WHERE", conn)
      assert is_binary(reason)
    end

    test "query returns error on non-existent table", %{conn: conn} do
      assert {:error, reason} =
               Dux.Connection.query("SELECT * FROM this_table_does_not_exist", conn)

      assert is_binary(reason)
    end
  end

  # ---------- Adversarial ----------

  describe "adversarial" do
    test "concurrent queries don't corrupt state", %{conn: conn} do
      Dux.Connection.execute("CREATE TABLE concurrent_test (id INTEGER)", conn)

      Dux.Connection.execute(
        "INSERT INTO concurrent_test SELECT * FROM range(1000)",
        conn
      )

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            result = Dux.Connection.query("SELECT COUNT(*) AS cnt FROM concurrent_test", conn)
            columns = Dux.Native.table_to_columns(result)
            assert columns["cnt"] == [1000]
            i
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert length(results) == 10
    end

    test "handles very long SQL", %{conn: conn} do
      # Generate a long UNION ALL query
      unions = Enum.map_join(1..100, " UNION ALL ", fn i -> "SELECT #{i} AS x" end)
      result = Dux.Connection.query(unions, conn)
      assert Dux.Native.table_n_rows(result) == 100
    end
  end
end
