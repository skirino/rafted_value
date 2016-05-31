use Croma
alias Croma.TypeGen, as: TG

defmodule RaftedValue.Leadership do
  alias RaftedValue.{PidSet, Config}

  use Croma.Struct, fields: [
    heartbeat_timer:      TG.nilable(Croma.Reference),
    quorum_timer:         TG.nilable(Croma.Reference),
    responding_followers: PidSet,
  ]

  defun new :: t do
    %__MODULE__{responding_followers: PidSet.new}
  end

  defun reset_heartbeat_timer(%__MODULE__{heartbeat_timer: timer} = l, %Config{heartbeat_timeout: timeout}) :: t do
    if timer, do: :gen_fsm.cancel_timer(timer)
    ref = :gen_fsm.send_event_after(timeout, :heartbeat_timeout)
    %__MODULE__{l | heartbeat_timer: ref}
  end

  defun cancel_heartbeat_timer(%__MODULE__{heartbeat_timer: timer} = l) :: t do
    if timer do
      :gen_fsm.cancel_timer(timer)
      %__MODULE__{l | heartbeat_timer: nil}
    else
      l
    end
  end
end
