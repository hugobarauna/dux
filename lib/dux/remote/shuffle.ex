defmodule Dux.Remote.Shuffle do
  @moduledoc """
  Distributed shuffle join for large-large joins.

  When both sides of a join are too large to broadcast, the shuffle:
  1. Hash-partitions both sides by join key across workers
  2. Ships each partition to its assigned worker (credit-based backpressure)
  3. Each worker joins its co-located partitions locally
  4. Coordinator merges results

  ## Usage

      Dux.Remote.Shuffle.execute(large_left, large_right,
        on: :join_key,
        workers: workers
      )

  Over-partitions by 4x the worker count to absorb moderate skew.
  """

  alias Dux.Remote.{Merger, Partitioner, Worker}

  @over_partition_factor 4
  @max_retries 3

  @doc """
  Execute a shuffle join between two large datasets.

  ## Options

    * `:on` — join column(s) (required)
    * `:how` — join type (default: `:inner`)
    * `:workers` — worker PIDs (default: all from `:pg`)
    * `:timeout` — per-operation timeout (default: `:infinity`)
  """
  def execute(%Dux{} = left, %Dux{} = right, opts) do
    execute_with_retry(left, right, opts, 0)
  end

  defp execute_with_retry(left, right, opts, attempt) do
    workers = Keyword.get_lazy(opts, :workers, &Worker.list/0)
    on = Keyword.fetch!(opts, :on)
    how = Keyword.get(opts, :how, :inner)
    timeout = Keyword.get(opts, :timeout, :infinity)

    on_col = to_string(on)
    n_workers = length(workers)
    n_buckets = n_workers * @over_partition_factor
    stage_id = :erlang.unique_integer([:positive])

    if workers == [] do
      raise ArgumentError, "no workers available for shuffle join"
    end

    try do
      # Phase 1: Hash-partition both sides on each worker
      # Each worker processes only its slice (1/N) of the data to avoid duplication
      left_sliced = slice_for_workers(left, workers)
      right_sliced = slice_for_workers(right, workers)

      left_partitions = hash_partition_all(left_sliced, on_col, n_buckets, timeout)
      right_partitions = hash_partition_all(right_sliced, on_col, n_buckets, timeout)

      # Phase 2: Shuffle exchange — send each bucket to the assigned worker
      # Assign buckets to workers round-robin
      bucket_to_worker = assign_buckets(n_buckets, workers)

      exchange_partitions(
        left_partitions,
        bucket_to_worker,
        workers,
        "shuffle_l_#{stage_id}",
        timeout
      )

      exchange_partitions(
        right_partitions,
        bucket_to_worker,
        workers,
        "shuffle_r_#{stage_id}",
        timeout
      )

      # Phase 3: Local join on each worker
      join_results =
        local_join_all(
          workers,
          "shuffle_l_#{stage_id}",
          "shuffle_r_#{stage_id}",
          on_col,
          how,
          timeout
        )

      # Phase 4: Merge on coordinator
      result =
        if join_results == [] do
          # No matching rows — return empty Dux
          Dux.from_query("SELECT 1 WHERE false") |> Dux.compute()
        else
          Merger.merge_to_dux(join_results, %Dux{ops: []})
        end

      # Cleanup
      cleanup(workers, stage_id)

      result
    rescue
      e ->
        cleanup(workers, stage_id)

        if attempt < @max_retries do
          execute_with_retry(left, right, opts, attempt + 1)
        else
          reraise e, __STACKTRACE__
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Phase 1: Hash partition
  # ---------------------------------------------------------------------------

  # Assign each worker a slice of the data to avoid duplication for replicated sources.
  # For Parquet globs, the Partitioner already handles this. For other sources,
  # we use NTILE to give each worker 1/N of the rows.
  defp slice_for_workers(%Dux{source: {:parquet, path, _}} = pipeline, workers)
       when is_binary(path) and byte_size(path) > 0 do
    # Parquet glob: use the existing partitioner
    Partitioner.assign(pipeline, workers)
  end

  defp slice_for_workers(pipeline, workers) do
    n = length(workers)

    workers
    |> Enum.with_index(1)
    |> Enum.map(fn {worker, idx} ->
      # Each worker gets rows where NTILE(N) == idx
      sliced =
        pipeline
        |> Dux.mutate_with(__slice: "NTILE(#{n}) OVER ()")
        |> Dux.filter_with("__slice = #{idx}")
        |> Dux.discard([:__slice])

      {worker, sliced}
    end)
  end

  defp hash_partition_all(worker_pipelines, on_col, n_buckets, timeout) do
    worker_pipelines
    |> Task.async_stream(
      fn {worker, pipeline} ->
        {:ok, buckets} = Worker.hash_partition(worker, pipeline, on_col, n_buckets, timeout)
        {worker, buckets}
      end,
      timeout: timeout,
      max_concurrency: length(worker_pipelines)
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  # ---------------------------------------------------------------------------
  # Phase 2: Shuffle exchange
  # ---------------------------------------------------------------------------

  defp assign_buckets(n_buckets, workers) do
    for bucket_id <- 0..(n_buckets - 1), into: %{} do
      worker_idx = rem(bucket_id, length(workers))
      {bucket_id, Enum.at(workers, worker_idx)}
    end
  end

  defp exchange_partitions(worker_partitions, bucket_to_worker, _workers, table_prefix, _timeout) do
    # For each source worker's partitions, send each non-empty bucket
    # to its assigned target worker
    tasks =
      for {_source_worker, buckets} <- worker_partitions,
          {bucket_id, ipc} <- buckets,
          ipc != nil do
        target_worker = Map.fetch!(bucket_to_worker, bucket_id)
        table_name = "#{table_prefix}_b#{bucket_id}"

        Task.async(fn ->
          Worker.append_chunk(target_worker, table_name, ipc)
        end)
      end

    Task.await_many(tasks, 60_000)
  end

  # ---------------------------------------------------------------------------
  # Phase 3: Local join
  # ---------------------------------------------------------------------------

  defp local_join_all(workers, left_prefix, right_prefix, on_col, how, timeout) do
    escaped_col = escape_ident(on_col)
    join_type = join_type_sql(how)

    # Each worker joins all its left buckets against all its right buckets
    workers
    |> Task.async_stream(
      fn worker ->
        # Find which buckets this worker owns
        info = Worker.info(worker)
        left_tables = info.tables |> Enum.filter(&String.starts_with?(&1, left_prefix))
        right_tables = info.tables |> Enum.filter(&String.starts_with?(&1, right_prefix))

        if left_tables == [] or right_tables == [] do
          # No data for this worker — return empty
          nil
        else
          # UNION ALL left buckets, UNION ALL right buckets, then join
          left_union =
            Enum.map_join(left_tables, " UNION ALL ", &~s(SELECT * FROM "#{escape_ident(&1)}"))

          right_union =
            Enum.map_join(right_tables, " UNION ALL ", &~s(SELECT * FROM "#{escape_ident(&1)}"))

          join_sql = """
            SELECT * FROM (#{left_union}) __left
            #{join_type} (#{right_union}) __right
            USING (#{escaped_col})
          """

          pipeline = Dux.from_query(join_sql)
          {:ok, ipc} = Worker.execute(worker, pipeline, timeout)
          ipc
        end
      end,
      timeout: timeout,
      max_concurrency: length(workers)
    )
    |> Enum.map(fn {:ok, result} -> result end)
    |> Enum.filter(& &1)
  end

  # ---------------------------------------------------------------------------
  # Cleanup
  # ---------------------------------------------------------------------------

  defp cleanup(workers, stage_id) do
    for worker <- workers do
      try do
        info = Worker.info(worker)

        info.tables
        |> Enum.filter(fn name ->
          String.contains?(name, "shuffle_l_#{stage_id}") or
            String.contains?(name, "shuffle_r_#{stage_id}")
        end)
        |> Enum.each(&Worker.drop_table(worker, &1))
      catch
        _, _ -> :ok
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp escape_ident(name), do: String.replace(name, ~s("), ~s(""))

  defp join_type_sql(:inner), do: "INNER JOIN"
  defp join_type_sql(:left), do: "LEFT JOIN"
  defp join_type_sql(:right), do: "RIGHT JOIN"
  defp join_type_sql(:cross), do: "CROSS JOIN"
end
