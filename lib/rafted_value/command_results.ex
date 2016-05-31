use Croma

defmodule RaftedValue.CommandResults do
  @type cmd_id :: RaftedValue.command_identifier
  @type t :: {:queue.queue(cmd_id), %{cmd_id => any}}
  defun validate(v :: any) :: Croma.Result.t(t) do
    {{l1, l2}, m} = t when is_list(l1) and is_list(l2) and is_map(m) -> {:ok, t}
    _ -> {:error, {:invalid_value, [__MODULE__]}}
  end

  defun new :: t do
    {:queue.new, %{}}
  end

  defun fetch({_q, m} :: t, cmd_id :: cmd_id) :: {:ok, any} | :error do
    Map.fetch(m, cmd_id)
  end

  defun put({q1, m1} :: t,
            cmd_id   :: cmd_id,
            result   :: any,
            max_size :: pos_integer) :: t do
    q2 = :queue.in(cmd_id, q1)
    m2 = Map.put(m1, cmd_id, result)
    if map_size(m2) <= max_size do
      {q2, m2}
    else
      {{:value, cmd_id_removed}, q3} = :queue.out(q2)
      m3 = Map.delete(m2, cmd_id_removed)
      {q3, m3}
    end
  end
end
