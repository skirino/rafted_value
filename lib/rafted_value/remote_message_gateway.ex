use Croma

defmodule RaftedValue.RemoteMessageGateway do
  @moduledoc """
  Replacement of `:gen_fsm.send_event/2` and `:gen_fsm.reply/2` as the default of `:communication_module`.

  This is introduced in order to work-around slow message passing to unreachable nodes of `:gen_fsm`, using temporary processes.
  In other words, it behaves in the same way as `:gen_server.cast/2` for unreachable nodes.
  """

  def send_event(fsm_ref, event) do
    do_send(fsm_ref, {:"$gen_event", event})
  end

  def reply({pid, tag}, reply) do
    do_send(pid, {tag, reply})
  end

  defp do_send(dest, msg) do
    case :erlang.send(dest, msg, [:noconnect]) do
      :noconnect ->
        spawn(:erlang, :send, [dest, msg])
        :ok
      _otherwise -> :ok
    end
  end
end
