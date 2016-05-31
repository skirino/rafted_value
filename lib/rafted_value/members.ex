use Croma
alias Croma.TypeGen, as: TG
alias Croma.Result, as: R

defmodule RaftedValue.Members do
  alias RaftedValue.{PidSet, LogEntry}

  use Croma.Struct, fields: [
    leader:             TG.nilable(Croma.Pid),
    all:                PidSet,               # replicated using raft logs (i.e. reproducible from logs)
    uncommitted_change: TG.nilable(LogEntry), # replicated using raft logs (i.e. reproducible from logs)
  ]

  defun new_for_lonely_leader :: t do
    %__MODULE__{leader: self, all: PidSet.put(PidSet.new, self)}
  end

  defun other_members_list(%__MODULE__{all: all}) :: [pid] do
    PidSet.delete(all, self) |> PidSet.to_list
  end

  defun put_leader(m :: t, leader_or_nil :: nil | pid) :: t do
    %__MODULE__{m | leader: leader_or_nil}
  end

  defun start_adding_follower(%__MODULE__{all: all, uncommitted_change: change} = m,
                              {_term, _index, :add_follower, new_follower} = entry) :: R.t(t) do
    if change do
      {:error, :previous_membership_change_not_yet_committed}
    else
      if PidSet.member?(all, new_follower) do
        {:error, :already_joined}
      else
        %__MODULE__{m | all: PidSet.put(all, new_follower), uncommitted_change: entry} |> R.pure
      end
    end
  end

  defun start_removing_follower(%__MODULE__{all: all, uncommitted_change: change} = m,
                                {_term, _index, :remove_follower, old_follower} = entry) :: R.t(t) do
    if old_follower == self do
      {:error, :cannot_remove_leader}
    else
      if change do
        {:error, :previous_membership_change_not_yet_committed}
      else
        if PidSet.member?(all, old_follower) do
          %__MODULE__{m | all: PidSet.delete(all, old_follower), uncommitted_change: entry} |> R.pure
        else
          {:error, :not_member}
        end
      end
    end
  end

  defun membership_change_committed(%__MODULE__{uncommitted_change: change} = m, index :: LogIndex.t) :: t do
    case change do
      {_, i, _, _} when i <= index -> %__MODULE__{m | uncommitted_change: nil}
      _                            -> m
    end
  end
end
