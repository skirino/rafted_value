use Croma
alias Croma.TypeGen, as: TG

defmodule RaftedValue.Election do
  alias RaftedValue.{PidSet, Members, Config}

  use Croma.Struct, fields: [
    voted_for: TG.nilable(Croma.Pid),
    votes:     TG.nilable(PidSet),
    timer:     TG.nilable(Croma.Reference),
  ]

  defun new_for_leader :: t do
    %__MODULE__{voted_for: self}
  end

  defun new_for_follower(config :: Config.t) :: t do
    %__MODULE__{timer: start_timer(config)}
  end

  defun replace_for_candidate(%__MODULE__{timer: timer}, config :: Config.t) :: t do
    if timer, do: :gen_fsm.cancel_timer(timer)
    votes = PidSet.new |> PidSet.put(self)
    %__MODULE__{voted_for: self, votes: votes, timer: start_timer(config)}
  end

  defun replace_for_follower(%__MODULE__{timer: timer}, config :: Config.t) :: t do
    if timer, do: :gen_fsm.cancel_timer(timer)
    new_for_follower(config)
  end

  defun vote_for(%__MODULE__{timer: timer} = e, candidate :: pid, config :: Config.t) :: t do
    if timer, do: :gen_fsm.cancel_timer(timer)
    %__MODULE__{e | voted_for: candidate, timer: start_timer(config)}
  end

  defun gain_vote(%__MODULE__{votes: votes} = e, %Members{all: all_members}, voter :: pid) :: {t, boolean} do
    new_votes = PidSet.put(votes, voter)
    new_election = %__MODULE__{e | votes: new_votes}
    majority? = PidSet.size(new_votes) >= div(PidSet.size(all_members), 2) + 1
    if majority? do
      {cancel_timer(new_election), true}
    else
      {new_election, false}
    end
  end

  defun reset_timer(%__MODULE__{timer: timer} = e, config :: Config.t) :: t do
    if timer, do: :gen_fsm.cancel_timer(timer)
    %__MODULE__{e | timer: start_timer(config)}
  end

  defun cancel_timer(%__MODULE__{timer: timer} = e) :: t do
    if timer do
      :gen_fsm.cancel_timer(timer)
      %__MODULE__{e | timer: nil}
    else
      e
    end
  end

  defunp start_timer(%Config{election_timeout: timeout}) :: reference do
    randomized_timeout = timeout + :rand.uniform(timeout)
    :gen_fsm.send_event_after(randomized_timeout, :election_timeout)
  end
end
