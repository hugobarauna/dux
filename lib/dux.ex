defmodule Dux do
  @moduledoc """
  DuckDB-native dataframe library for Elixir.

  The `Dux` module IS the dataframe. All operations are verbs on `%Dux{}` structs.
  Pipelines are lazy — operations accumulate as an AST until `compute/1` compiles
  them to SQL CTEs and executes against DuckDB.

  ## Example

      Dux.from_parquet("data.parquet")
      |> Dux.filter(year > 2024)
      |> Dux.mutate(revenue: price * quantity)
      |> Dux.group_by(:region)
      |> Dux.summarise(total: sum(revenue))
      |> Dux.compute()

  """

  defstruct [:source, ops: [], names: [], dtypes: %{}, groups: []]

  @type source ::
          {:parquet, String.t()}
          | {:csv, String.t(), keyword()}
          | {:ndjson, String.t(), keyword()}
          | {:table, reference()}
          | {:sql, String.t()}

  @type t :: %__MODULE__{
          source: source(),
          ops: [tuple()],
          names: [String.t()],
          dtypes: %{String.t() => atom() | tuple()},
          groups: [String.t()]
        }
end
