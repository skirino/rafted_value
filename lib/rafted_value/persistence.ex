use Croma

defmodule RaftedValue.Persistence do
  alias RaftedValue.{TermNumber, LogIndex, LogEntry}

  defmodule SnapshotInfo do
    use Croma.Struct, fields: [
      path:  Croma.String,
      term:  TermNumber,
      index: LogIndex,
      size:  Croma.PosInteger,
    ]
  end

  use Croma.Struct, fields: [
    dir:              Croma.String,
    log_fd:           Croma.Tuple,
    log_size_written: Croma.NonNegInteger,
    latest_snapshot:  Croma.TypeGen.nilable(SnapshotInfo),
  ]

  defun new_for_dir(dir :: Path.t) :: t do
    File.mkdir_p!(dir)
    %__MODULE__{dir: dir, log_fd: open_log_file(dir), log_size_written: 0}
  end

  defp open_log_file(dir) do
    {{y, mon, d}, {h, min, s}} = :erlang.universaltime()
    basename = "log.#{y}-#{pad2(mon)}-#{pad2(d)}T#{pad2(h)}:#{pad2(min)}:#{pad2(s)}"
    File.open!(Path.join(dir, basename), [:write, :sync, :raw])
  end

  defunp pad2(int :: non_neg_integer) :: String.t do
    i when i <  10 -> "0#{i}"
    i when i < 100 -> Integer.to_string(i)
  end

  defun write_log_entries(%__MODULE__{log_fd: fd, log_size_written: size} = p, entries :: [LogEntry.t]) :: t do
    bin = Enum.map(entries, &LogEntry.to_binary/1) |> :erlang.iolist_to_binary()
    :ok = :file.write(fd, bin)
    %__MODULE__{p | log_size_written: size + byte_size(bin)}
  end
end
