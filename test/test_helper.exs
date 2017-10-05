ExUnit.start()

defmodule JustAnInt do
  @behaviour RaftedValue.Data
  def new(_), do: 0
  def command(i, :get     ), do: {i, i    }
  def command(i, {:set, j}), do: {i, j    }
  def command(i, :inc     ), do: {i, i + 1}
  def query(i, :get), do: i
  def to_snapshot(i), do: i
  def from_snapshot(i, _env), do: i

  def to_disk(val, path) do
  	bin = :erlang.term_to_binary(val) |> :zlib.gzip()
  	File.write!(path, bin)
  end
  def from_disk(path, _env) do
  	File.read!(path) |> :zlib.gunzip() |> :erlang.binary_to_term()
  end
end
