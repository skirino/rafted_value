use Croma

defmodule RaftedValue.Communication do
  @callback cast(server :: GenServer.server, msg :: any) :: :ok

  @callback reply(from :: GenServer.from, reply :: any) :: :ok
end

defmodule RaftedValue.RemoteMessageGateway do
  @moduledoc """
  Default implementation of `:communication_module`.

  Essentially `cast/2` behaves as `:gen_statem.cast/2` and `reply/2` behaves as `:gen_statem.reply/2`.

  This is introduced in order to work-around slow message passing to unreachable nodes.
  In other words, it uses a temporary process to send message to a not-yet-connected node,
  as in the same way as e.g. `:gen_server.cast/2`.
  """

  @behaviour RaftedValue.Communication

  def cast(fsm_ref, event) do
    do_send(fsm_ref, {:"$gen_cast", event})
  end

  # This function is kept for backward compatibility for `< 0.8.0`; will be removed in `0.9.0`.
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
