use Croma

defmodule RaftedValue.Leadership do
  alias RaftedValue.{PidSet, Members, Config, Monotonic}

  use Croma.Struct, fields: [
    heartbeat_timer:          Croma.Reference,
    quorum_timer:             Croma.Reference,
    quorum_timer_started_at:  Monotonic,
    follower_responded_times: Croma.Map, # %{pid => Monotonic.t} : latest `:leader_timestamp`s each follower responded to
  ]

  defun new_for_leader(config :: Config.t) :: t do
    %__MODULE__{
      heartbeat_timer:          start_heartbeat_timer(config),
      quorum_timer:             start_quorum_timer(config),
      quorum_timer_started_at:  Monotonic.millis(),
      follower_responded_times: %{},
    }
  end

  defun reset_heartbeat_timer(%__MODULE__{heartbeat_timer: timer} = l, config :: Config.t) :: t do
    :gen_fsm.cancel_timer(timer)
    %__MODULE__{l | heartbeat_timer: start_heartbeat_timer(config)}
  end
  defun reset_quorum_timer(%__MODULE__{quorum_timer: timer} = l, config :: Config.t) :: t do
    :gen_fsm.cancel_timer(timer)
    %__MODULE__{l | quorum_timer: start_quorum_timer(config), quorum_timer_started_at: Monotonic.millis()}
  end

  defunp start_heartbeat_timer(%Config{heartbeat_timeout: timeout}) :: reference do
    :gen_fsm.send_event_after(timeout, :heartbeat_timeout)
  end
  defunp start_quorum_timer(config :: Config.t) :: reference do
    :gen_fsm.send_event_after(max_election_timeout(config), :cannot_reach_quorum)
  end

  defun follower_responded(%__MODULE__{quorum_timer_started_at: started_at, follower_responded_times: times} = l,
                           %Members{all: all} = members,
                           follower  :: pid,
                           timestamp :: Monotonic.t,
                           config    :: Config.t) :: t do
    follower_pids = PidSet.delete(all, self()) |> PidSet.to_list
    new_times =
      Map.update(times, follower, timestamp, &max(&1, timestamp))
      |> Map.take(follower_pids) # filter by the actual members, in order not to be disturbed by message from already-removed member
    new_leadership = %__MODULE__{l | follower_responded_times: new_times}
    case quorum_last_reached_at(new_leadership, members) do
      nil        -> new_leadership
      reached_at ->
        if started_at < reached_at do
          reset_quorum_timer(new_leadership, config)
        else
          new_leadership
        end
    end
  end

  defun stop_timers(%__MODULE__{heartbeat_timer: t1, quorum_timer: t2}) :: :ok do
    :gen_fsm.cancel_timer(t1)
    :gen_fsm.cancel_timer(t2)
    :ok
  end

  defun unresponsive_followers(%__MODULE__{follower_responded_times: times},
                               members :: Members.t,
                               config :: Config.t) :: [pid] do
    since = Monotonic.millis() - max_election_timeout(config)
    Members.other_members_list(members)
    |> Enum.filter(fn pid ->
      case times[pid] do
        nil -> true
        t   -> t < since
      end
    end)
  end

  defun can_safely_remove?(%__MODULE__{} = l, %Members{all: all} = members, follower :: pid, config :: Config.t) :: boolean do
    unhealthy_followers = unresponsive_followers(l, members, config)
    if follower in unhealthy_followers do
      true # unhealthy follower can always be safely removed
    else
      # healthy follower can be removed if remaining members can reach majority
      n_members_after_remove         = PidSet.size(all) - 1
      n_healthy_members_after_remove = n_members_after_remove - length(unhealthy_followers)
      n_healthy_members_after_remove * 2 > n_members_after_remove
    end
  end

  defun remove_follower_response_time_entry(%__MODULE__{follower_responded_times: times} = leadership, follower :: pid) :: t do
    %__MODULE__{leadership | follower_responded_times: Map.delete(times, follower)}
  end

  defun lease_expired?(leadership :: t,
                       members    :: Members.t,
                       %Config{election_timeout: timeout,
                               election_timeout_clock_drift_margin: margin}) :: boolean do
    case quorum_last_reached_at(leadership, members) do
      nil        -> true
      reached_at -> reached_at + timeout - margin <= Monotonic.millis()
    end
  end

  defunp quorum_last_reached_at(%__MODULE__{follower_responded_times: times}, %Members{all: all}) :: nil | integer do
    n = PidSet.size(all)
    if n <= 1 do
      nil
    else
      n_half_followers = div(n - 1, 2)
      Map.values(times)
      |> Enum.sort
      |> Enum.drop(n_half_followers)
      |> List.first # can return `nil` if `follwer_responded_times` doesn't contain enough items (i.e. right after a leader is elected)
    end
  end

  defunp max_election_timeout(%Config{election_timeout: t}) :: pos_integer do
    t * 2
  end
end
