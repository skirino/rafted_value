defmodule RaftedValue.Command do
  @moduledoc """
  TODO: Write something
  """

  @type data :: any
  @type arg  :: any
  @type ret  :: any

  @callback new :: data
  @callback command(data, arg) :: {ret, data}
end
