defmodule RaftedValue.Data do
  @moduledoc """
  TODO: Write something
  """

  @type data        :: any
  @type command_arg :: any
  @type command_ret :: any
  @type query_arg   :: any
  @type query_ret   :: any

  @callback new :: data
  @callback command(data, command_arg) :: {command_ret, data}
  @callback query(data, query_arg) :: query_ret
end
