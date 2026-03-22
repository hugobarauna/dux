defmodule Dux.Telemetry do
  @moduledoc """
  Telemetry events emitted by Dux.

  Dux uses `:telemetry` to emit events at key points during query execution,
  distributed coordination, graph algorithms, and IO. Attach handlers to
  observe performance, build dashboards, or log progress.

  ## Quick start

      :telemetry.attach_many("dux-logger", [
        [:dux, :query, :stop],
        [:dux, :distributed, :worker, :stop]
      ], fn event, measurements, metadata, _config ->
        IO.puts("[Dux] \#{inspect(event)} took \#{measurements.duration}ns - \#{inspect(metadata)}")
      end, nil)

  ## Core events

  ### `[:dux, :query, :start]`

  Emitted when `Dux.compute/1` begins executing a pipeline.

  | Measurement | Type | Description |
  |------------|------|-------------|
  | `:system_time` | integer | System time at start (via `telemetry:span`) |

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:n_ops` | integer | Number of pipeline operations |
  | `:distributed` | boolean | Whether execution is distributed |

  ### `[:dux, :query, :stop]`

  Emitted when `Dux.compute/1` completes successfully.

  | Measurement | Type | Description |
  |------------|------|-------------|
  | `:duration` | integer | Duration in native time units |

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:n_ops` | integer | Number of pipeline operations |
  | `:distributed` | boolean | Whether execution was distributed |
  | `:n_rows` | integer | Number of rows in the result |

  ### `[:dux, :query, :exception]`

  Emitted when `Dux.compute/1` raises an exception.

  | Measurement | Type | Description |
  |------------|------|-------------|
  | `:duration` | integer | Duration in native time units |

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:kind` | atom | `:error`, `:exit`, or `:throw` |
  | `:reason` | term | The exception or error |
  | `:stacktrace` | list | The stacktrace |

  ## Distributed events

  ### `[:dux, :distributed, :fan_out, :start | :stop]`

  Emitted around the worker fan-out phase.

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:n_workers` | integer | Number of workers |

  ### `[:dux, :distributed, :worker, :stop]`

  Emitted each time an individual worker completes its partition.

  | Measurement | Type | Description |
  |------------|------|-------------|
  | `:duration` | integer | Worker execution time (native units) |
  | `:ipc_bytes` | integer | Size of Arrow IPC result |

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:worker` | pid | Worker PID |
  | `:worker_index` | integer | 0-based index in the worker list |
  | `:n_workers` | integer | Total number of workers |

  ### `[:dux, :distributed, :merge, :start | :stop]`

  Emitted around the merge phase on the coordinator.

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:n_results` | integer | Number of partial results being merged |

  ### `[:dux, :distributed, :broadcast, :start | :stop]`

  Emitted when broadcasting a table to workers (join routing).

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:table_name` | string | Broadcast table name |
  | `:n_workers` | integer | Number of target workers |
  | `:ipc_bytes` | integer | Size of serialized table |

  ### `[:dux, :distributed, :shuffle, :start | :stop]`

  Emitted around a full shuffle join.

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:n_workers` | integer | Number of workers |
  | `:n_buckets` | integer | Number of hash buckets |

  ## Graph events

  ### `[:dux, :graph, :algorithm, :start | :stop]`

  Emitted around any graph algorithm execution.

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:algorithm` | atom | `:pagerank`, `:connected_components`, `:shortest_paths`, `:triangle_count` |
  | `:n_vertices` | integer | Number of vertices |
  | `:n_edges` | integer | Number of edges |
  | `:distributed` | boolean | Whether using distributed workers |

  ### `[:dux, :graph, :iteration, :stop]`

  Emitted after each iteration of iterative algorithms (PageRank, Connected Components).

  | Measurement | Type | Description |
  |------------|------|-------------|
  | `:duration` | integer | Duration of this iteration |

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:algorithm` | atom | `:pagerank` or `:connected_components` |
  | `:iteration` | integer | Current iteration (1-based) |
  | `:max_iterations` | integer | Maximum iterations configured |
  | `:converged` | boolean | Whether the algorithm converged this iteration |

  ## IO events

  ### `[:dux, :io, :write, :start | :stop]`

  Emitted around file write operations.

  | Metadata | Type | Description |
  |----------|------|-------------|
  | `:format` | atom | `:csv`, `:parquet`, or `:ndjson` |
  | `:path` | string | Output file path |
  """
end
