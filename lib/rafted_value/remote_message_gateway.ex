use Croma

defmodule RaftedValue.RemoteMessageGateway do
  @moduledoc """
  Default implementation of `:communication_module`.

  This is introduced in order to work-around slow message passing to unreachable nodes.
  In other words, it uses a temporary process to send message to a not-yet-connected node, as in the same way as `:gen_server.cast/2`.

  Essentially `send_event/2` behaves as `:gen_statem.cast/2` and `reply/2` behaves as `:gen_statem.reply/2`.
  The discrepancy in the name of `send_event` comes from a historical reason (`:gen_fsm` had been used).
  """

  def cast(fsm_ref, event) do
    do_send(fsm_ref, {:"$gen_cast", event})
  end

  def send_event(fsm_ref, event) do
    do_send(fsm_ref, {:"$gen_cast", event})
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
