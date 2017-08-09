use Croma
alias Croma.TypeGen, as: TG

defmodule RaftedValue.Election do
  alias RaftedValue.{PidSet, Members, Config, Monotonic, Timer}

  use Croma.Struct, fields: [
    voted_for:         TG.nilable(Croma.Pid),
    votes:             TG.nilable(PidSet),
    timer:             TG.nilable(Croma.Reference),
    leader_message_at: TG.nilable(Monotonic),
  ]

  defun new_for_leader :: t do
    %__MODULE__{voted_for: self()}
  end

  defun new_for_follower(config :: Config.t) :: t do
    %__MODULE__{timer: start_timer(config), leader_message_at: Monotonic.millis()}
  end

  defun update_for_candidate(%__MODULE__{timer: timer} = e, config :: Config.t) :: t do
    if timer, do: Timer.cancel(timer)
    votes = PidSet.new() |> PidSet.put(self())
    %__MODULE__{e | voted_for: self(), votes: votes, timer: start_timer(config)}
  end

  defun update_for_follower(%__MODULE__{timer: timer} = e, config :: Config.t) :: t do
    if timer, do: Timer.cancel(timer)
    %__MODULE__{e | voted_for: nil, votes: nil, timer: start_timer(config)}
  end

  defun vote_for(%__MODULE__{timer: timer} = e, candidate :: pid, config :: Config.t) :: t do
    if timer, do: Timer.cancel(timer)
    %__MODULE__{e | voted_for: candidate, timer: start_timer(config)}
  end

  defun gain_vote(%__MODULE__{votes: votes, timer: timer} = e, %Members{all: all_members}, voter :: pid) :: {t, boolean} do
    new_votes = PidSet.put(votes, voter)
    majority? = PidSet.size(new_votes) >= div(PidSet.size(all_members), 2) + 1
    if majority? do
      Timer.cancel(timer) # this function is called during `:candidate` state, in which `timer` is always on
      {%__MODULE__{e | votes: new_votes, timer: nil}, true}
    else
      {%__MODULE__{e | votes: new_votes}, false}
    end
  end

  defun reset_timer(%__MODULE__{timer: timer} = e, config :: Config.t) :: t do
    if timer, do: Timer.cancel(timer)
    %__MODULE__{e | timer: start_timer(config), leader_message_at: Monotonic.millis()}
  end

  defunp start_timer(%Config{election_timeout: timeout}) :: reference do
    randomized_timeout = timeout + :rand.uniform(timeout)
    Timer.make(randomized_timeout, :election_timeout)
  end

  defun minimum_timeout_elapsed_since_last_leader_message?(%__MODULE__{leader_message_at: t},
                                                           %Config{election_timeout: timeout,
                                                                   election_timeout_clock_drift_margin: margin}) :: boolean do
    case t do
      nil -> true
      t   -> t + timeout - margin <= Monotonic.millis()
    end
  end
end
