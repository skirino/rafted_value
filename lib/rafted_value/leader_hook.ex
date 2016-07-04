defmodule RaftedValue.LeaderHook do
  @moduledoc """
  A behaviour module for hooks that are invoked in leader.

  Note that there are cases where hooks are invoked multiple times for a single event due to leader change.
  """

  alias RaftedValue.Data
  @type neglected :: any

  @doc """
  Hook to be called when a command submitted by `RaftedValue.command/4` is committed.
  """
  @callback on_command_committed(
    data_before_command :: Data.data,
    command_arg         :: Data.command_arg,
    command_ret         :: Data.command_ret,
    data_after_command  :: Data.data) :: neglected

  @doc """
  Hook to be called when a query given by `RaftedValue.query/3` is executed.
  """
  @callback on_query_answered(
    data      :: Data.data,
    query_arg :: Data.query_arg,
    query_ret :: Data.query_ret) :: neglected

  @doc """
  Hook to be called when a new follower is added to a consensus group
  by `RaftedValue.start_link/2` with `:join_existing_consensus_group` specified.
  """
  @callback on_follower_added(Data.data, pid) :: neglected

  @doc """
  Hook to be called when a follower is removed from a consensus group by `RaftedValue.remove_follower/2`.
  """
  @callback on_follower_removed(Data.data, pid) :: neglected

  @doc """
  Hook to be called when a new leader is elected in a consensus group.
  """
  @callback on_elected(Data.data) :: neglected
end

defmodule RaftedValue.LeaderHook.NoOp do
  @behaviour RaftedValue.LeaderHook
  def on_command_committed(_, _, _, _), do: nil
  def on_query_answered(_, _, _), do: nil
  def on_follower_added(_, _), do: nil
  def on_follower_removed(_, _), do: nil
  def on_elected(_), do: nil
end
