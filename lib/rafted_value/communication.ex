use Croma

defmodule RaftedValue.Communication do
  @callback cast(server :: GenServer.server, msg :: any) :: :ok

  @callback reply(from :: GenServer.from, reply :: any) :: :ok
end

defmodule RaftedValue.RemoteMessageGateway do
  @moduledoc """
  Default implementation of `:communication_module`.

  Essentially `cast/2` behaves as `:gen_statem.cast/2` and `reply/2` behaves as `:gen_statem.reply/2`.

  This is introduced in order to work-around slow message passing to unreachable nodes;
  if the target node is not connected to `Node.self()`, functions in this node give up delivering message.
  In order to recover from temporary network issues, reconnecting to disconnected nodes should be tried elsewhere.
  Note that `:raft_fleet` (since 0.5.0) periodically tries to re-establish inter-node connections out of the box.
  """

  @behaviour RaftedValue.Communication

  def cast(fsm_ref, event) do
    do_send(fsm_ref, {:"$gen_cast", event})
  end

  def reply({pid, tag}, reply) do
    do_send(pid, {tag, reply})
  end

  defp do_send(dest, msg) do
    :erlang.send(dest, msg, [:noconnect])
    :ok
  end
end
