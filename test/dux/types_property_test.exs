defmodule Dux.TypesPropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  # The Connection is started by the application supervisor.
  # We use it directly for these tests.

  setup_all do
    db = Dux.Connection.get_db()
    %{db: db}
  end

  describe "integer round-trip" do
    property "integers survive query → extract", %{db: db} do
      check all(i <- integer(-1_000_000..1_000_000)) do
        table = Dux.Native.df_query(db, "SELECT #{i}::BIGINT AS val")
        %{"val" => [result]} = Dux.Native.table_to_columns(table)
        assert result == i
      end
    end
  end

  describe "float round-trip" do
    property "finite floats survive query → extract", %{db: db} do
      check all(f <- float(min: -1.0e10, max: 1.0e10)) do
        table = Dux.Native.df_query(db, "SELECT #{f}::DOUBLE AS val")
        %{"val" => [result]} = Dux.Native.table_to_columns(table)
        assert_in_delta result, f, abs(f) * 1.0e-10 + 1.0e-15
      end
    end
  end

  describe "string round-trip" do
    property "strings survive query → extract", %{db: db} do
      check all(s <- string(:printable, min_length: 0, max_length: 200)) do
        # Escape single quotes for SQL
        escaped = String.replace(s, "'", "''")
        table = Dux.Native.df_query(db, "SELECT '#{escaped}'::VARCHAR AS val")
        %{"val" => [result]} = Dux.Native.table_to_columns(table)
        assert result == s
      end
    end
  end

  describe "boolean round-trip" do
    property "booleans survive query → extract", %{db: db} do
      check all(b <- boolean()) do
        sql_bool = if b, do: "true", else: "false"
        table = Dux.Native.df_query(db, "SELECT #{sql_bool}::BOOLEAN AS val")
        %{"val" => [result]} = Dux.Native.table_to_columns(table)
        assert result == b
      end
    end
  end

  describe "null handling" do
    property "nulls are preserved across all types", %{db: db} do
      types = ["BIGINT", "DOUBLE", "VARCHAR", "BOOLEAN", "DATE"]

      check all(type <- member_of(types)) do
        table = Dux.Native.df_query(db, "SELECT NULL::#{type} AS val")
        %{"val" => [result]} = Dux.Native.table_to_columns(table)
        assert result == nil
      end
    end
  end

  describe "Arrow IPC round-trip" do
    property "integer data survives IPC encode → decode", %{db: db} do
      check all(values <- list_of(integer(-10_000..10_000), min_length: 1, max_length: 100)) do
        unions = Enum.map_join(values, " UNION ALL ", &"SELECT #{&1}::BIGINT AS val")
        table = Dux.Native.df_query(db, unions)

        ipc = Dux.Native.table_to_ipc(table)
        restored = Dux.Native.table_from_ipc(ipc)

        original_cols = Dux.Native.table_to_columns(table)
        restored_cols = Dux.Native.table_to_columns(restored)

        assert original_cols == restored_cols
      end
    end

    property "string data survives IPC encode → decode", %{db: db} do
      check all(
              values <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 50),
                  min_length: 1,
                  max_length: 50
                )
            ) do
        unions =
          Enum.map_join(values, " UNION ALL ", fn s ->
            escaped = String.replace(s, "'", "''")
            "SELECT '#{escaped}'::VARCHAR AS val"
          end)

        table = Dux.Native.df_query(db, unions)
        ipc = Dux.Native.table_to_ipc(table)
        restored = Dux.Native.table_from_ipc(ipc)

        assert Dux.Native.table_to_columns(table) == Dux.Native.table_to_columns(restored)
      end
    end
  end

  describe "dtype mapping" do
    test "all core DuckDB types map to known Dux dtypes", %{db: db} do
      table =
        Dux.Native.df_query(db, """
          SELECT
            1::TINYINT AS ti,
            1::SMALLINT AS si,
            1::INTEGER AS i,
            1::BIGINT AS bi,
            1::UTINYINT AS uti,
            1::USMALLINT AS usi,
            1::UINTEGER AS ui,
            1::UBIGINT AS ubi,
            1.0::FLOAT AS f32,
            1.0::DOUBLE AS f64,
            true AS b,
            'hello'::VARCHAR AS s,
            '2025-01-01'::DATE AS d,
            NULL::BLOB AS blob
        """)

      dtypes = Dux.Native.table_dtypes(table)
      dtype_map = Map.new(dtypes)

      assert dtype_map["ti"] == {:s, 8}
      assert dtype_map["si"] == {:s, 16}
      assert dtype_map["i"] == {:s, 32}
      assert dtype_map["bi"] == {:s, 64}
      assert dtype_map["uti"] == {:u, 8}
      assert dtype_map["usi"] == {:u, 16}
      assert dtype_map["ui"] == {:u, 32}
      assert dtype_map["ubi"] == {:u, 64}
      assert dtype_map["f32"] == {:f, 32}
      assert dtype_map["f64"] == {:f, 64}
      assert dtype_map["b"] == :boolean
      assert dtype_map["s"] == :string
      assert dtype_map["d"] == :date
      assert dtype_map["blob"] == :binary
    end
  end
end
