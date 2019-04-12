use Croma

defmodule RaftedValue.PidSet do
  @moduledoc false

  @type t :: %{pid => true}
  defun valid?(v :: any) :: boolean, do: is_map(v)

  defun new()                         :: t              , do: %{}
  defun size(set :: t)                :: non_neg_integer, do: map_size(set)
  defun put(set :: t, pid :: pid)     :: t              , do: Map.put(set, pid, true)
  defun delete(set :: t, pid :: pid)  :: t              , do: Map.delete(set, pid)
  defun member?(set :: t, pid :: pid) :: boolean        , do: Map.has_key?(set, pid)
  defun to_list(set :: t)             :: [pid]          , do: Map.keys(set)
  defun from_list(l :: [pid])         :: t              , do: Enum.reduce(l, new(), &put(&2, &1))
end
