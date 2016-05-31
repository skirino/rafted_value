use Croma

defmodule RaftedValue.PidSet do
  @moduledoc false

  @type t :: %{pid => true}
  defun validate(v :: any) :: Croma.Result.t(t) do
    m when is_map(m) -> {:ok, m}
    _                -> {:error, {:invalid_value, [__MODULE__]}}
  end

  defun new                           :: t              , do: %{}
  defun size(set :: t)                :: non_neg_integer, do: map_size(set)
  defun put(set :: t, pid :: pid)     :: t              , do: Map.put(set, pid, true)
  defun delete(set :: t, pid :: pid)  :: t              , do: Map.delete(set, pid)
  defun to_list(set :: t)             :: [pid]          , do: Map.keys(set)
  defun member?(set :: t, pid :: pid) :: boolean        , do: Map.has_key?(set, pid)
end
