use Croma
alias Croma.TypeGen, as: TG

defmodule RaftedValue.Leadership do
  alias RaftedValue.{PidSet, Members, Config}

  use Croma.Struct, fields: [
    heartbeat_timer:          TG.nilable(Croma.Reference),
    quorum_timer:             TG.nilable(Croma.Reference),
    quorum_timer_started_at:  TG.nilable(Croma.Integer),
    follower_responded_times: Croma.Map,
  ]

  defun new :: t do
    %__MODULE__{follower_responded_times: %{}}
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

  defun reset_quorum_timer(%__MODULE__{quorum_timer: timer} = l, config :: Config.t, now :: integer \\ monotonic_millis) :: t do
    if timer, do: :gen_fsm.cancel_timer(timer)
    ref = :gen_fsm.send_event_after(max_election_timeout(config), :cannot_reach_quorum)
    %__MODULE__{l | quorum_timer: ref, quorum_timer_started_at: now}
  end

  defun follower_responded(%__MODULE__{quorum_timer_started_at: started_at, follower_responded_times: times} = l,
                           %Members{all: all},
                           follower :: pid,
                           config   :: Config.t) :: t do
    now = monotonic_millis
    new_times =
      Map.put(times, follower, now)
      |> Map.take(PidSet.delete(all, self) |> PidSet.to_list)
    new_leadership = %__MODULE__{l | follower_responded_times: new_times}
    n_responded_since_timer_set = Enum.count(new_times, fn {_, time} -> started_at < time end)
    if (n_responded_since_timer_set + 1) * 2 > PidSet.size(all) do
      reset_quorum_timer(new_leadership, config, now)
    else
      new_leadership
    end
  end

  defun deactivate(%__MODULE__{heartbeat_timer: t1, quorum_timer: t2} = l) :: t do
    if t1, do: :gen_fsm.cancel_timer(t1)
    if t2, do: :gen_fsm.cancel_timer(t2)
    %__MODULE__{l | heartbeat_timer: nil, quorum_timer: nil}
  end

  defun unresponsive_followers(%__MODULE__{follower_responded_times: times},
                               config :: Config.t) :: [pid] do
    since = monotonic_millis - max_election_timeout(config)
    Enum.filter_map(times, fn {_, t} -> t < since end, fn {pid, _} -> pid end)
  end

  defunp monotonic_millis :: integer do
    System.monotonic_time(:milli_seconds)
  end

  defunp max_election_timeout(%Config{election_timeout: t}) :: pos_integer do
    t * 2
  end
end
