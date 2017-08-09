use Croma

defmodule RaftedValue.Timer do
  defun make(time :: non_neg_integer, msg :: any) :: reference do
#    :erlang.start_timer(time, self(), {:"$gen_cast", msg})
    :erlang.send_after(time, self(), {:"$gen_cast", msg})
  end

  defun cancel(ref :: reference) :: :ok do
    case :erlang.cancel_timer(ref) do
      false ->
        receive do
          {:timeout, ^ref, _} -> :ok
        after
          0 -> :ok
        end
      _remaining_time -> :ok
    end
  end
end
