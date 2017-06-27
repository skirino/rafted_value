use Croma

defmodule RaftedValue.Snapshot do
  alias RaftedValue.{Members, TermNumber, LogEntry, Config, CommandResults, Persistence}
  alias RaftedValue.Persistence.SnapshotMetadata

  use Croma.Struct, fields: [
    members:              Members,
    term:                 TermNumber,
    last_committed_entry: LogEntry,
    config:               Config,
    data:                 Croma.Any,
    command_results:      CommandResults,
  ]

  defun read_lastest_snapshot_and_logs_if_available(dir :: Path.t) :: nil | {t, SnapshotMetadata.t, Enum.t(LogEntry.t)} do
    case find_snapshot_and_log_files(dir) do
      nil                              -> nil
      {snapshot_path, meta, log_paths} ->
        snapshot = File.read!(snapshot_path) |> decode()
        {_, last_committed_index, _, _} = snapshot.last_committed_entry
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

  defun encode(snapshot :: Snapshot.t) :: binary do
    :erlang.term_to_binary(snapshot) |> :zlib.gzip()
  end

  defun decode(bin :: binary) :: Snapshot.t do
    :zlib.gunzip(bin) |> :erlang.binary_to_term()
  end
end
