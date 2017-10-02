use Croma

defmodule RaftedValue.LogEntry do
  alias RaftedValue.{TermNumber, LogIndex, Config}
  @type t :: {TermNumber.t, LogIndex.t, :command           , {GenServer.from, Data.command_arg, reference}}
           | {TermNumber.t, LogIndex.t, :query             , {GenServer.from, Data.query_arg}}
           | {TermNumber.t, LogIndex.t, :leader_elected    , pid}
           | {TermNumber.t, LogIndex.t, :add_follower      , pid}
           | {TermNumber.t, LogIndex.t, :remove_follower   , pid}
           | {TermNumber.t, LogIndex.t, :restore_from_files, pid}

  defun valid?(v :: any) :: boolean do
    {_, _, _, _} -> true
    _            -> false
  end

  defp entry_type_to_tag(:command           ), do: 0
  defp entry_type_to_tag(:query             ), do: 1
  defp entry_type_to_tag(:leader_elected    ), do: 3
  defp entry_type_to_tag(:add_follower      ), do: 4
  defp entry_type_to_tag(:remove_follower   ), do: 5
  defp entry_type_to_tag(:restore_from_files), do: 6

  defp tag_to_entry_type(0), do: {:ok, :command           }
  defp tag_to_entry_type(1), do: {:ok, :query             }
  defp tag_to_entry_type(3), do: {:ok, :leader_elected    }
  defp tag_to_entry_type(4), do: {:ok, :add_follower      }
  defp tag_to_entry_type(5), do: {:ok, :remove_follower   }
  defp tag_to_entry_type(6), do: {:ok, :restore_from_files}
  defp tag_to_entry_type(_), do: :error

  defun to_binary({term, index, entry_type, others} :: t) :: binary do
    bin = :erlang.term_to_binary(others)
    size = byte_size(bin)
    <<term :: size(64), index :: size(64), entry_type_to_tag(entry_type) :: size(8), size :: size(64), bin :: binary, size :: size(64)>>
  end

  defunpt extract_from_binary(bin :: binary) :: nil | {t, rest :: binary} do
    with <<term :: size(64), index :: size(64), type_tag :: size(8), size1 :: size(64)>> <> rest1 <- bin,
         {:ok, entry_type} = tag_to_entry_type(type_tag),
         <<others_bin :: binary-size(size1), size2 :: size(64)>> <> rest2 <- rest1 do
      if size1 == size2 do
        {{term, index, entry_type, :erlang.binary_to_term(others_bin)}, rest2}
      else
        raise "redundant size information in log entry not matched"
      end
    else
      _ -> nil # insufficient input, can be retried with subsequent binary data
    end
  end

  defunp extract_multiple_from_binary(bin :: binary) :: {[t], rest :: binary} do
    extract_multiple_from_binary_impl(bin, [])
  end

  defp extract_multiple_from_binary_impl(bin, acc) do
    case extract_from_binary(bin) do
      nil           -> {Enum.reverse(acc), bin}
      {entry, rest} -> extract_multiple_from_binary_impl(rest, [entry | acc])
    end
  end

  def read_as_stream(log_path) do
    File.stream!(log_path, [], 4096)
    |> Stream.transform(<<>>, fn(bin, carryover) ->
      extract_multiple_from_binary(carryover <> bin)
    end)
  end

  defun read_last_entry_index(log_path :: Path.t) :: nil | LogIndex.t do
    case :file.open(log_path, [:raw, :binary]) do
      {:ok, f}    -> read_last_entry_index_impl(f, File.stat!(log_path).size)
      {:error, _} -> nil
    end
  end

  defp read_last_entry_index_impl(f, size) do
    if size < 8 do
      nil
    else
      {:ok, <<binsize1 :: size(64)>>} = :file.pread(f, size - 8, 8)
      last_entry_start_offset = size - binsize1 - 33 # (term: 8, index: 8, tag: 1, size1: 8, size2: 8)
      if last_entry_start_offset < 0 do
        nil
      else
        {:ok, <<_term :: size(64), index :: size(64), type_tag :: size(8), binsize2 :: size(64)>>} = :file.pread(f, last_entry_start_offset, 25)
        case tag_to_entry_type(type_tag) do # minimal sanity checking
          {:ok, _} when binsize2 == binsize1 -> index
          _                                  -> nil
        end
      end
    end
  end
end
