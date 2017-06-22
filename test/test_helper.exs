ExUnit.start()

defmodule JustAnInt do
  @behaviour RaftedValue.Data
  def new(), do: 0
  def command(i, :get     ), do: {i, i    }
  def command(i, {:set, j}), do: {i, j    }
  def command(i, :inc     ), do: {i, i + 1}
  def query(i, :get), do: i
end
