use Croma
alias Croma.TypeGen, as: TG

defmodule RaftedValue.Leadership do
  alias RaftedValue.{PidSet, Members, Config}

  use Croma.Struct, fields: [
    heartbeat_timer:      TG.nilable(Croma.Reference),
    quorum_timer:         TG.nilable(Croma.Reference),
    responding_followers: PidSet,
  ]

  defun new :: t do
    %__MODULE__{responding_followers: PidSet.new}
  end

  defun new_for_leader(config :: Config.t) :: t do
    new
    |> reset_heartbeat_timer(config)
    |> reset_quorum_timer(config)
  end

  defun reset_heartbeat_timer(%__MODULE__{heartbeat_timer: timer} = l, %Config{heartbeat_timeout: timeout}) :: t do
    if timer, do: :gen_fsm.cancel_timer(timer)
    ref = :gen_fsm.send_event_after(timeout, :heartbeat_timeout)
    %__MODULE__{l | heartbeat_timer: ref}
  end

  defun reset_quorum_timer(%__MODULE__{quorum_timer: timer} = l, %Config{election_timeout: election_timeout}) :: t do
    if timer, do: :gen_fsm.cancel_timer(timer)
    max_election_timeout = election_timeout * 2
    ref = :gen_fsm.send_event_after(max_election_timeout, :cannot_reach_quorum)
    %__MODULE__{l | quorum_timer: ref, responding_followers: PidSet.new}
  end

  defun follower_responded(%__MODULE__{responding_followers: followers} = l,
                           %Members{all: all},
                           follower :: pid,
                           config   :: Config.t) :: t do
    new_followers = PidSet.put(followers, follower)
    if (PidSet.size(new_followers) + 1) * 2 > PidSet.size(all) do
      reset_quorum_timer(l, config)
    else
      %__MODULE__{l | responding_followers: new_followers}
    end
  end

  defun deactivate(%__MODULE__{heartbeat_timer: t1, quorum_timer: t2} = l) :: t do
    if t1, do: :gen_fsm.cancel_timer(t1)
    if t2, do: :gen_fsm.cancel_timer(t2)
    %__MODULE__{l | heartbeat_timer: nil, quorum_timer: nil}
  end
end
