defmodule Dux.Connection do
  @moduledoc false

  # GenServer managing a single DuckDB connection per node.
  # Serializes all DuckDB access (mutex).
  # DuckDB's morsel-driven parallelism saturates all cores internally.

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end
end
