ExUnit.start()

defmodule JustAnInt do
  @behaviour RaftedValue.Data
  def new(), do: 0
  def command(i, :get     ), do: {i, i    }
  def command(i, {:set, j}), do: {i, j    }
  def command(i, :inc     ), do: {i, i + 1}
  def query(i, :get), do: i
end

defmodule MessageSendingHook do
  @behaviour RaftedValue.LeaderHook
  def on_command_committed(_, _, _, _), do: nil
  def on_query_answered(_, _, _)      , do: nil
  def on_follower_added(_, pid)       , do: send(:test_runner, {:follower_added, pid})
  def on_follower_removed(_, pid)     , do: send(:test_runner, {:follower_removed, pid})
  def on_elected(_)                   , do: send(:test_runner, {:elected, self()})
  def on_restored_from_files(_)       , do: send(:test_runner, {:restored_from_files, self()})
end
