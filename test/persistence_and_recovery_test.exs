defmodule RaftedValue.PersistenceAndRecoveryTest do
  use ExUnit.Case
  alias RaftedValue.{LogEntry, Snapshot, Persistence}

  @config  RaftedValue.make_config(JustAnInt, [])
  @name    :foo
  @tmp_dir "tmp"

  setup do
    File.rm_rf!(@tmp_dir)
    on_exit(fn ->
      File.rm_rf!(@tmp_dir)
    end)
  end

  defp read_all_live_log_entries(i_committed) do
    log_paths = Persistence.find_log_files_containing_uncommitted_entries(@tmp_dir, i_committed)
    all_entries =
      Enum.flat_map(log_paths, fn path ->
        entries = LogEntry.read_as_stream(path) |> Enum.to_list()
        {_, index_first, _, _} = hd(entries)
        assert index_first == extract_first_index_from_log_path(path)
        entries
      end)
    Enum.map(all_entries, fn {_, i, _, _} -> i end)
    |> Enum.chunk(2, 1)
    |> Enum.each(fn [i1, i2] ->
      assert i2 - i1 == 1
    end)
    all_entries
  end

  defp extract_first_index_from_log_path(path) do
    ["log", index_first] = Path.basename(path) |> String.split("_")
    String.to_integer(index_first)
  end

  defp in_memory_and_disk_logs_same?(index_range) do
    logs = :sys.get_state(@name) |> elem(1) |> Map.fetch!(:logs)
    assert logs.i_min .. logs.i_max == index_range
    assert Enum.sort(Map.keys(logs.map)) == Enum.to_list(index_range)
    i_committed = index_range.first
    m_disk =
      read_all_live_log_entries(i_committed)
      |> Enum.drop_while(fn {_, i, _, _} -> i < i_committed end)
      |> Map.new(fn {_, i, _, _} = e -> {i, e} end)
    # `logs.map` may contain extra entries which have been discarded in disk by log compaction
    assert m_disk == Map.take(logs.map, Map.keys(m_disk))
  end

  defp read_snapshot(path) do
    File.read!(path) |> Snapshot.decode()
  end

  defp snapshot_path_to_committed_index(path) do
    ["snapshot", _term, index] = Path.basename(path) |> String.split("_")
    String.to_integer(index)
  end

  test "persist logs and snapshots and recover by reading them" do
    {:ok, _} = RaftedValue.start_link({:create_new_consensus_group, @config}, @name, @tmp_dir)
    assert in_memory_and_disk_logs_same?(1..1)
    assert :gen_fsm.stop(@name) == :ok
    snapshot_path1 = Path.join(@tmp_dir, "snapshot_0_1")
    assert %Snapshot{} = read_snapshot(snapshot_path1)
    snapshot_committed_index1 = 1

    {:ok, _} = RaftedValue.start_link({:create_new_consensus_group, @config}, @name, @tmp_dir)
    assert in_memory_and_disk_logs_same?(snapshot_committed_index1 .. snapshot_committed_index1 + 1)
    assert RaftedValue.command(@name, :inc) == {:ok, 0}
    assert in_memory_and_disk_logs_same?(1..3)
    assert :gen_fsm.stop(@name) == :ok

    {:ok, _} = RaftedValue.start_link({:create_new_consensus_group, @config}, @name, @tmp_dir)
    assert in_memory_and_disk_logs_same?(snapshot_committed_index1 .. snapshot_committed_index1 + 3)
    Enum.each(1..20, fn i ->
      assert RaftedValue.command(@name, :inc) == {:ok, i}
    end)
    assert in_memory_and_disk_logs_same?(snapshot_committed_index1 .. snapshot_committed_index1 + 23)
    assert :gen_fsm.stop(@name) == :ok
    [snapshot_path2] = Path.wildcard(Path.join(@tmp_dir, "snapshot_*")) |> List.delete(snapshot_path1)
    assert %Snapshot{} = read_snapshot(snapshot_path2)
    snapshot_committed_index2 = snapshot_path_to_committed_index(snapshot_path2)

    {:ok, _} = RaftedValue.start_link({:create_new_consensus_group, @config}, @name, @tmp_dir)
    assert in_memory_and_disk_logs_same?(snapshot_committed_index2 .. snapshot_committed_index1 + 24)
    assert RaftedValue.query(@name, :get) == {:ok, 21}
    assert :gen_fsm.stop(@name) == :ok
  end

  test "uncommitted logs should be committed by lonely leader immediately after recovery" do
    {:ok, l} = RaftedValue.start_link({:create_new_consensus_group, @config}, nil, @tmp_dir)
    {:ok, f} = RaftedValue.start_link({:join_existing_consensus_group, [l]}, nil, nil)
    assert MapSet.new(RaftedValue.status(l).members) == MapSet.new([l, f])
    assert :gen_fsm.stop(f) == :ok

    # Now incoming commands won't be committed
    Enum.each(1..3, fn _ ->
      assert RaftedValue.command(l, :inc, 100) == {:error, :timeout}
    end)
    assert :gen_fsm.stop(l) == :ok

    # Restore from snapshot, commands should be applied
    {:ok, _} = RaftedValue.start_link({:create_new_consensus_group, @config}, @name, @tmp_dir)
    assert RaftedValue.query(@name, :get) == {:ok, 3}
    assert :gen_fsm.stop(@name) == :ok
  end

  test "follower should replicate log entries and store them in disk with de-duplication" do
    dir_l = Path.join(@tmp_dir, "l")
    dir_f = Path.join(@tmp_dir, "f")
    {:ok, l} = RaftedValue.start_link({:create_new_consensus_group, @config}, nil, dir_l)
    {:ok, f} = RaftedValue.start_link({:join_existing_consensus_group, [l]} , nil, dir_f)

    # In case leader hasn't received AppendEntriesResponse from the follower, the leader re-sends part of log entries.
    # (This can happen also in non-persisting setup but is much more frequent in persisting setup as followers must flush log entries before replying to its leader)
    assert RaftedValue.command(l, :inc) == {:ok, 0}

    # The follower should de-duplicate the received log entries before writing them to disk.
    [log_path] = Path.wildcard(Path.join(dir_f, "log_*"))
    entries = LogEntry.read_as_stream(log_path) |> Enum.to_list()
    assert entries == Enum.uniq(entries)

    assert :gen_fsm.stop(f) == :ok
    assert :gen_fsm.stop(l) == :ok
  end

  test "non-persisting and persisting members can interchange snapshots with each other" do
    {:ok, n1} = RaftedValue.start_link({:create_new_consensus_group, @config})
    Enum.each(0 .. 10, fn i ->
      assert RaftedValue.command(n1, :inc) == {:ok, i}
    end)
    # send snapshot: `n1` => `p1`
    {:ok, p1} = RaftedValue.start_link({:join_existing_consensus_group, [n1]}, nil, @tmp_dir)
    Enum.each(11 .. 20, fn i ->
      assert RaftedValue.command(n1, :inc) == {:ok, i}
    end)
    assert :gen_fsm.stop(p1) == :ok
    assert :gen_fsm.stop(n1) == :ok

    # recover from disk snapshot
    {:ok, p2} = RaftedValue.start_link({:create_new_consensus_group, @config}, nil, @tmp_dir)
    Enum.each(21 .. 30, fn i ->
      assert RaftedValue.command(p2, :inc) == {:ok, i}
    end)
    # send snapshot: `p2` => `n2`
    {:ok, n2} = RaftedValue.start_link({:join_existing_consensus_group, [p2]})
    Enum.each(31 .. 40, fn i ->
      assert RaftedValue.command(p2, :inc) == {:ok, i}
    end)

    assert :gen_fsm.stop(n2) == :ok
    assert :gen_fsm.stop(p2) == :ok
  end
end
