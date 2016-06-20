use Croma

defmodule RaftedValue.Leadership do
  alias RaftedValue.{PidSet, Members, Config}

  use Croma.Struct, fields: [
    heartbeat_timer:          Croma.Reference,
    quorum_timer:             Croma.Reference,
    quorum_timer_started_at:  Croma.Integer,
    follower_responded_times: Croma.Map, # %{pid => integer}
  ]

  defun new_for_leader(config :: Config.t) :: t do
    %__MODULE__{
      heartbeat_timer:          start_heartbeat_timer(config),
      quorum_timer:             start_quorum_timer(config),
      quorum_timer_started_at:  monotonic_millis,
      follower_responded_times: %{},
    }
  end

  defun reset_heartbeat_timer(%__MODULE__{heartbeat_timer: timer} = l, config :: Config.t) :: t do
    :gen_fsm.cancel_timer(timer)
    %__MODULE__{l | heartbeat_timer: start_heartbeat_timer(config)}
  end
  defun reset_quorum_timer(%__MODULE__{quorum_timer: timer} = l, config :: Config.t, now :: integer \\ monotonic_millis) :: t do
    :gen_fsm.cancel_timer(timer)
    %__MODULE__{l | quorum_timer: start_quorum_timer(config), quorum_timer_started_at: now}
  end

  defunp start_heartbeat_timer(%Config{heartbeat_timeout: timeout}) :: reference do
    :gen_fsm.send_event_after(timeout, :heartbeat_timeout)
  end
  defunp start_quorum_timer(config :: Config.t) :: reference do
    :gen_fsm.send_event_after(max_election_timeout(config), :cannot_reach_quorum)
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

  defun stop_timers(%__MODULE__{heartbeat_timer: t1, quorum_timer: t2}) :: :ok do
    :gen_fsm.cancel_timer(t1)
    :gen_fsm.cancel_timer(t2)
    :ok
  end

  defun unresponsive_followers(%__MODULE__{follower_responded_times: times},
                               config :: Config.t) :: [pid] do
    since = monotonic_millis - max_election_timeout(config)
    Enum.filter_map(times, fn {_, t} -> t < since end, fn {pid, _} -> pid end)
  end

  defun can_safely_remove?(%__MODULE__{} = l, %Members{all: all}, follower :: pid, config :: Config.t) :: boolean do
    unhealthy_followers = unresponsive_followers(l, config)
    if follower in unhealthy_followers do
      true # unhealthy follower can always be safely removed
    else
      # healthy follower can be removed if remaining members can reach majority
      n_members_after_remove         = PidSet.size(all) - 1
      n_healthy_members_after_remove = n_members_after_remove - length(unhealthy_followers)
      n_healthy_members_after_remove * 2 > n_members_after_remove
    end
  end

  defun minimum_timeout_elapsed_since_quorum_responded?(%__MODULE__{quorum_timer_started_at: t},
                                                        %Config{election_timeout: timeout}) :: boolean do
    t + timeout < monotonic_millis
  end

  defunp monotonic_millis :: integer do
    System.monotonic_time(:milli_seconds)
  end

  defunp max_election_timeout(%Config{election_timeout: t}) :: pos_integer do
    t * 2
  end
end
