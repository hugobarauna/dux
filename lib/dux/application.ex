defmodule Dux.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # {Dux.Connection, []}
    ]

    opts = [strategy: :one_for_one, name: Dux.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
