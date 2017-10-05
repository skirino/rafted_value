use Croma

defmodule RaftedValue.Persistence do
  alias RaftedValue.{Config, TermNumber, LogIndex, LogEntry, Snapshot, SnapConsensus}

  defmodule SnapshotMetadata do
    use Croma.Struct, fields: [
      path:                 Croma.String,
      term:                 TermNumber,
      last_committed_index: LogIndex,
      size:                 Croma.PosInteger,
    ]
  end

  defmodule PersistenceHook do
    alias RaftedValue.Snapshot
    @type neglected      :: any
    @type path           :: Path.t
    @type consensus_term :: integer
    @type index          :: integer
    @type size           :: integer
    @doc """
    Hook to be called when a snapshot is finished.
    """
    @callback snapshot_created(path, consensus_term, index, size) :: neglected
  end
  defmodule PersistenceHookNoOp do
    @behaviour RaftedValue.Persistence.PersistenceHook
    
    def snapshot_created(_path, _term, _index, _size), do: nil
  end

  use Croma.Struct, fields: [
    dir:                      Croma.String,
    hook:                     Croma.Atom,
    log_fd:                   Croma.TypeGen.nilable(Croma.Tuple), # This field is `nil` only during initialization (within `new_with_initial_snapshotting/4`)
    log_size_written:         Croma.NonNegInteger,
    log_compaction_factor:      Croma.Number,
    latest_snapshot_metadata: Croma.TypeGen.nilable(SnapshotMetadata), # This field is `nil` only between startup and first snapshot
    snapshot_writer:          Croma.TypeGen.nilable(Croma.Pid),
  ]

  defun new_with_initial_snapshotting(dir :: Path.t, factor :: number, persistence_hook :: atom, snapshot :: Snapshot.t) :: t do
    File.mkdir_p!(dir)
    {_, index_first, _, _} = entry_elected = snapshot.consensus.last_committed_entry
    %__MODULE__{dir: dir, log_size_written: 0, log_compaction_factor: factor, hook: persistence_hook} # `log_fd` will be filled soon
    |> switch_log_file_and_spawn_snapshot_writer(snapshot, index_first)
    |> write_log_entries([entry_elected])
  end

  defun new_with_disk_snapshot(dir :: Path.t, factor :: number, persistence_hook :: atom, meta :: SnapshotMetadata.t, {_, index_first, _, _} = entry_restore :: LogEntry.t) :: t do
    %__MODULE__{dir: dir, log_fd: open_log_file(dir, index_first), log_size_written: 0, log_compaction_factor: factor, hook: persistence_hook, latest_snapshot_metadata: meta}
    |> write_log_entries([entry_restore])
  end

  defun new_with_snapshot_sent_from_leader(dir :: Path.t, factor :: number, persistence_hook :: atom, snapshot :: Snapshot.t) :: t do
    File.mkdir_p!(dir)
    {_, index_snapshot, _, _} = snapshot.consensus.last_committed_entry
    %__MODULE__{dir: dir, log_size_written: 0, log_compaction_factor: factor, hook: persistence_hook} # `log_fd` will be filled soon
    |> switch_log_file_and_spawn_snapshot_writer(snapshot, index_snapshot + 1)
  end

  defun unset_snapshot_metadata(p :: t) :: t do
    %__MODULE__{p | latest_snapshot_metadata: nil}
  end

  defun write_log_entries(%__MODULE__{log_fd: fd, log_size_written: size} = p, entries :: [LogEntry.t]) :: t do
    bin = Enum.map(entries, &LogEntry.to_binary/1) |> :erlang.iolist_to_binary()
    :ok = :file.write(fd, bin)
    %__MODULE__{p | log_size_written: size + byte_size(bin)}
  end

  defun log_compaction_runnable?(%__MODULE__{latest_snapshot_metadata: meta,
                                             log_size_written:         size_l,
                                             log_compaction_factor:      factor,
                                             snapshot_writer:          writer}) :: boolean do
    if is_pid(writer) do
      false
    else
      case meta do
        nil                             -> true
        %SnapshotMetadata{size: size_s} -> size_s * factor < size_l
      end
    end
  end

  defun switch_log_file_and_spawn_snapshot_writer(%__MODULE__{dir: dir, log_fd: log_fd1} = persistence,
                                                  snapshot   :: Snapshot.t,
                                                  index_next :: LogIndex.t) :: t do
    if log_fd1 do
      :ok = :file.close(log_fd1)
    end
    server_pid  = self()
    log_fd2     = open_log_file(dir, index_next)
    {pid, _ref} = spawn_monitor(fn ->
      write_snapshot(snapshot, dir, server_pid)
    end)
    %__MODULE__{persistence | log_fd: log_fd2, log_size_written: 0, snapshot_writer: pid}
  end

  defp open_log_file(dir, index_next) do
    log_path = Path.join(dir, "log_#{index_next}")
    File.open!(log_path, [:write, :sync, :raw])
  end

  defp write_snapshot(%Snapshot{data:                 data,
                                consensus: %SnapConsensus{
                                  config:  %Config{data_module: data_module},
                                  last_committed_entry: {_, last_committed_index, _, _},
                                  term:                 term}} = snapshot,
                      dir,
                      server_pid) do
    snapshot_basename = "snapshot_#{term}_#{last_committed_index}"
    snapshot_dir      = Path.join(dir, snapshot_basename)
    File.mkdir(snapshot_dir)

    # Write the value
    vpath = value_path(snapshot_dir)
    data_module.to_disk(data, vpath)

    # Write the consensus 
    cpath                = consensus_path(snapshot_dir)
    consensus_compressed = SnapConsensus.encode(snapshot.consensus)
    File.write!(cpath, consensus_compressed)
    

    %{size: consensus_size} = File.stat! cpath

    # notify the gen_statem process (we have to wait for reply in order to ensure that older snapshots won't be used anymore)
    message = {:snapshot_created, snapshot_dir, term, last_committed_index, consensus_size}
    :ok = :gen_statem.call(server_pid, message, :infinity)

    # cleanup obsolete snapshots and logs
    Path.wildcard(Path.join(dir, "snapshot_*"))
    |> Enum.filter(fn path -> Path.basename(path) != snapshot_basename end)
    |> Enum.drop(1)
    |> Enum.each(&File.rm_rf!/1)

    find_log_files_with_committed_entries_only(dir, last_committed_index)
    |> Enum.each(&File.rm!/1)
  end

  defunp find_log_files_with_committed_entries_only(dir :: Path.t, i_committed :: LogIndex.t) :: [Path.t] do
    partition_obsolete_and_live_log_files(dir, i_committed)
    |> elem(0)
    |> Enum.map(&elem(&1, 0))
  end

  defun find_log_files_containing_uncommitted_entries(dir :: Path.t, i_committed :: LogIndex.t) :: [Path.t] do
    partition_obsolete_and_live_log_files(dir, i_committed)
    |> elem(1)
    |> Enum.map(&elem(&1, 0))
  end

  defunp partition_obsolete_and_live_log_files(dir :: Path.t, i_committed :: LogIndex.t) :: {[{Path.t, LogIndex.t}], [{Path.t, LogIndex.t}]} do
    Path.wildcard(Path.join(dir, "log_*"))
    |> Enum.map(fn path -> {extract_first_log_index_from_path(path), path} end)
    |> Enum.sort()
    |> Enum.chunk(2, 1, [nil])
    |> Enum.map(fn
      [{_, path}, {i_next, _}] -> {path, i_next - 1}
      [{_, path}, nil        ] -> {path, :infinity } # atom is larger than any integers
    end)
    |> Enum.split_with(fn {_, i} -> i < i_committed end)
  end

  defun read_last_log_index(dir :: Path.t) :: nil | LogIndex.t do
    case Path.wildcard(Path.join(dir, "log_*")) do
      []    -> nil
      paths ->
        latest_log_path = Enum.max_by(paths, &extract_first_log_index_from_path/1)
        LogEntry.read_last_entry_index(latest_log_path)
    end
  end

  defunp extract_first_log_index_from_path(path :: Path.t) :: LogIndex.t do
    ["log", index_first] = Path.basename(path) |> String.split("_")
    String.to_integer(index_first)
  end

  defun read_lastest_snapshot_from_dir(data_environment :: any, dir :: Path.t) :: SnapShot.t do
    consensus = consensus_path(dir) |> File.read!() |> SnapConsensus.decode()
    %Config{data_module: data_module} = consensus.config
    value = value_path(dir) |> data_module.from_disk(data_environment)
    %Snapshot{consensus: consensus, data: value}
  end

  defun value_path(dir :: Path.t) :: Path.t do
    Path.join(dir, "value.snap")
  end
  defun consensus_path(dir :: Path.t) :: Path.t do
    Path.join(dir, "consensus.snap")
  end
end
