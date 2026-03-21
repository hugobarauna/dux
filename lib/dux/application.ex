defmodule Dux.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Process groups for worker discovery
      %{id: :pg, start: {:pg, :start_link, []}},
      {Dux.Connection, []},
      {Dux.Remote.LocalGC, []},
      {DynamicSupervisor, name: Dux.Remote.HolderSupervisor, strategy: :one_for_one}
    ]

    opts = [strategy: :one_for_one, name: Dux.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
