defmodule RaftedValue.LogEntryTest do
  use Croma.TestCase

  defp make_gen_server_from() do
    {self(), make_ref()}
  end

  test "LogEntry.t <=> binary representation" do
    [
      {1, 2, :command        , {make_gen_server_from(), :some_command_arg, make_ref()}},
      {1, 2, :query          , {make_gen_server_from(), :some_query_arg}},
      {1, 2, :change_config  , RaftedValue.make_config(__MODULE__)},
      {1, 2, :leader_elected , self()},
      {1, 2, :add_follower   , self()},
      {1, 2, :remove_follower, self()},
    ] |> Enum.each(fn entry ->
      binary = LogEntry.to_binary(entry)
      [
        <<>>,
        :crypto.strong_rand_bytes(16),
      ] |> Enum.each(fn rest ->
        assert LogEntry.extract_from_binary(binary <> rest) == {entry, rest}
      end)
    end)
  end

  test "extract_from_binary should report error on failure" do
    bin = :erlang.term_to_binary(self())
    [
      <<>>,
      <<1 :: size(64), 2 :: size(64), 6 :: size(8), byte_size(bin)       :: size(64), bin :: binary>>, # invalid tag
      <<1 :: size(64), 2 :: size(64), 3 :: size(8), byte_size(bin) + 10  :: size(64), bin :: binary>>, # insufficient data
      <<1 :: size(64), 2 :: size(64), 3 :: size(8), byte_size("invalid") :: size(64), "invalid">>    , # :erlang.binary_to_term/1 fails
    ] |> Enum.each(fn b ->
      assert LogEntry.extract_from_binary(b) == nil
    end)
  end
end
