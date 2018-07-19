use Croma

defmodule RaftedValue.Config do
  use Croma.Struct, fields: [
    data_module:                         Croma.Atom,
    leader_hook_module:                  Croma.Atom,
    communication_module:                Croma.Atom,
    heartbeat_timeout:                   Croma.PosInteger,
    election_timeout:                    Croma.PosInteger, # minimum value; actual timeout is randomly picked from `election_timeout .. 2 * election_timeout`
    election_timeout_clock_drift_margin: Croma.PosInteger,
    max_retained_command_results:        Croma.PosInteger,
  ]
end

defmodule RaftedValue.TermNumber do
  use Croma.SubtypeOfInt, min: 0
end

defmodule RaftedValue.LogIndex do
  use Croma.SubtypeOfInt, min: 0
end

defmodule RaftedValue.LogInfo do
  use Croma.SubtypeOfTuple, elem_modules: [RaftedValue.TermNumber, RaftedValue.LogIndex]
end

defmodule RaftedValue.FollowerIndices do
  defmodule Pair do
    use Croma.SubtypeOfTuple, elem_modules: [RaftedValue.LogIndex, RaftedValue.LogIndex]
  end
  use Croma.SubtypeOfMap, key_module: Croma.Pid, value_module: Pair
end

defmodule RaftedValue.Monotonic do
  @type t :: integer
  defun valid?(v :: term) :: boolean, do: is_integer(v)

  defun millis() :: t do
    System.monotonic_time(:millisecond)
  end
end

defmodule RaftedValue.AddFollowerError do
  defexception [:message, :pid]
end
