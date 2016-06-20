defmodule RaftedValue.Data do
  @moduledoc """
  A behaviour that defines data to be replicated by `RaftedValue` servers.

  It is required to implement a callback module of this behaviour to run a consensus group.
  Concrete implementations of all callback functions of this behaviour must be [pure](https://en.wikipedia.org/wiki/Pure_function).
  """

  @type data        :: any
  @type command_arg :: any
  @type command_ret :: any
  @type query_arg   :: any
  @type query_ret   :: any

  @doc """
  Creates an initial value to be stored.

  Whenever a new consensus group is started by `RaftedValue.start_link/2` (called with `:create_new_consensus_group`),
  this function is called to initialize the stored value.
  """
  @callback new :: data

  @doc """
  Generic read/write operation on the stored value.

  This callback function is invoked by `RaftedValue.command/4`.
  Commands are replicated across members of the consensus group and executed in all members
  in order to reproduce the same history of stored value.
  This function should return a pair of "return value to client" and "next version of stored data".
  """
  @callback command(data, command_arg) :: {command_ret, data}

  @doc """
  Read-only operation on the stored value.

  This callback function is invoked by `RaftedValue.query/3`.
  This function should return "return value to client".
  """
  @callback query(data, query_arg) :: query_ret
end
