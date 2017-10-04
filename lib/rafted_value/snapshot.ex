use Croma
defmodule RaftedValue.SnapConsensus do
alias RaftedValue.{Members, TermNumber, LogEntry, Config, CommandResults}

  use Croma.Struct, fields: [
    config:               Config,
    members:              Members,
    term:                 TermNumber,
    last_committed_entry: LogEntry,
    command_results:      CommandResults,
  ]
  defun encode(consensus :: t) :: binary do
    :erlang.term_to_binary(consensus)# |> :zlib.gzip()
  end

  defun decode(bin :: binary) :: t do
    #:zlib.gunzip(bin) |> :erlang.binary_to_term()
    :erlang.binary_to_term(bin)
  end
end
defmodule RaftedValue.Snapshot do
  alias RaftedValue.{LogEntry, Config, Persistence, SnapConsensus, Transfer}
  
  alias RaftedValue.Persistence.SnapshotMetadata
  alias RaftedValue.RPC.{InstallSnapshot, InstallSnapshotCompressed}

  use Croma.Struct, fields: [
    consensus:            SnapConsensus,
    data:                 Croma.Any,
  ]

  @transfer_size 1024 * 1024

  defun from_install_snapshot(data_environment :: any, is :: InstallSnapshot.t) :: t do
    %Config{data_module: data_module} = is.config
    %__MODULE__{
      data: data_module.from_snapshot(is.data, data_environment),
      consensus: %SnapConsensus{
        config: is.config,
        members: is.members,
        term: is.term,
        last_committed_entry: is.last_committed_entry,
        command_results: is.command_results
      }
    }
  end

  defun from_compressed_snapshot(data_environment :: any, sc :: InstallSnapshotCompressed.t) :: t do
    %InstallSnapshotCompressed{consensus_bin: consensus_bin, value_io_pid: value_io_pid} = sc

    consensus            = consensus_bin |> SnapConsensus.decode()
    config               = consensus.config
    {temp_path, _}       = System.cmd("mktemp", [])
    value_path           = String.trim(temp_path, "\n")
    value_destination    = File.stream!(value_path, [:write])
    %Config{data_module: data_module} = config
    
    #  Read the remote file 1mB at a time
    Transfer.stream(value_io_pid, @transfer_size)
    |> Enum.into(value_destination)
    
    data = data_module.from_disk(value_path, data_environment)

    File.rm!(value_path)

    %__MODULE__{
      consensus: consensus,
      data: data
    }
  end

  defun read_lastest_snapshot_and_logs_if_available(data_environment :: any, dir :: Path.t) :: nil | {t, SnapshotMetadata.t, Enum.t(LogEntry.t)} do
    case find_snapshot_and_log_files(dir) do
      nil                              -> nil
      {snapshot_dir, meta, log_paths} ->
        snapshot = Persistence.read_lastest_snapshot_from_dir(data_environment, snapshot_dir)
        {_, last_committed_index, _, _} = snapshot.consensus.last_committed_entry
        log_stream =
          Stream.flat_map(log_paths, &LogEntry.read_as_stream/1)
          |> Stream.drop_while(fn {_, i, _, _} -> i < last_committed_index end)
        {snapshot, meta, log_stream}
    end
  end

  defunp find_snapshot_and_log_files(dir :: Path.t) :: nil | {Path.t, SnapshotMetadata.t, [Path.t]} do
    case Path.wildcard(Path.join(dir, "snapshot_*")) |> List.last() do
      nil           -> nil
      snapshot_path ->
        ["snapshot", term_str, last_index_str] = Path.basename(snapshot_path) |> String.split("_")
        last_committed_index = String.to_integer(last_index_str)
        %File.Stat{size: size} = File.stat!(snapshot_path)
        meta = %SnapshotMetadata{path: snapshot_path, term: String.to_integer(term_str), last_committed_index: last_committed_index, size: size}
        log_paths = Persistence.find_log_files_containing_uncommitted_entries(dir, last_committed_index)
        {snapshot_path, meta, log_paths}
    end
  end


end
