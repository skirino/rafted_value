defmodule RaftedValue.LogEntryTest do
  use Croma.TestCase
  alias RaftedValue.Persistence

  @tmp_dir "tmp"

  setup do
    File.rm_rf!(@tmp_dir)
    on_exit(fn ->
      File.rm_rf!(@tmp_dir)
    end)
  end

  defp make_gen_server_from() do
    {self(), make_ref()}
  end

  defp make_entries_list() do
    [
      {1, 2, :command        , {make_gen_server_from(), :some_command_arg, make_ref()}},
      {1, 2, :query          , {make_gen_server_from(), :some_query_arg}},
      {1, 2, :change_config  , RaftedValue.make_config(__MODULE__)},
      {1, 2, :leader_elected , self()},
      {1, 2, :add_follower   , self()},
      {1, 2, :remove_follower, self()},
    ]
  end

  test "LogEntry.t <=> binary representation" do
    make_entries_list() |> Enum.each(fn entry ->
      binary = LogEntry.to_binary(entry)
      [
        <<>>,
        :crypto.strong_rand_bytes(16),
      ] |> Enum.each(fn rest ->
        assert LogEntry.extract_from_binary(binary <> rest) == {entry, rest}
      end)
    end)
  end

  test "extract_from_binary/1 should report error on failure" do
    bin = :erlang.term_to_binary(self())
    [
      <<>>,
      <<1 :: size(64), 2 :: size(64), 3 :: size(8), byte_size(bin) + 10  :: size(64), bin :: binary>>, # insufficient data
    ] |> Enum.each(fn b ->
      assert LogEntry.extract_from_binary(b) == nil
    end)

    [
      <<1 :: size(64), 2 :: size(64), 6 :: size(8), byte_size(bin)       :: size(64), bin :: binary>>, # invalid tag
      <<1 :: size(64), 2 :: size(64), 3 :: size(8), byte_size("invalid") :: size(64), "invalid">>    , # :erlang.binary_to_term/1 fails
    ] |> Enum.each(fn b ->
      catch_error LogEntry.extract_from_binary(b)
    end)
  end

  test "read_as_stream/1" do
    dir = Path.join(@tmp_dir, "log_entry_test")
    persistence1 = Persistence.new_for_dir(dir)
    l = make_entries_list()
    entries = Enum.map(1..100, fn _ -> Enum.random(l) end)
    persistence2 = Persistence.write_log_entries(persistence1, entries)
    assert persistence2.log_size_written >= 4096

    [log_path] = Path.wildcard(Path.join(dir, "log.*"))
    stream = LogEntry.read_as_stream(log_path)
    assert Enum.to_list(stream) == entries
  end
end
