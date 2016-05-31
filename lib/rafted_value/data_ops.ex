defmodule RaftedValue.DataOps do
  @moduledoc """
  TODO: Write something
  """

  @type data        :: any
  @type command_arg :: any
  @type ret         :: any

  @callback new :: data
  @callback command(data, command_arg) :: {ret, data}
end
