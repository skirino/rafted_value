use Croma
alias Croma.TypeGen, as: TG
alias Croma.Result, as: R

defmodule RaftedValue.Members do
  alias RaftedValue.{PidSet, LogEntry}

  use Croma.Struct, fields: [
    leader:                        TG.nilable(Croma.Pid),
    all:                           PidSet,                # replicated using raft logs (i.e. reproducible from logs)
    uncommitted_membership_change: TG.nilable(LogEntry),  # replicated using raft logs (i.e. reproducible from logs)
    pending_leader_change:         TG.nilable(Croma.Pid),
  ]

  defun new_for_lonely_leader :: t do
    %__MODULE__{leader: self, all: PidSet.put(PidSet.new, self)}
  end

  defun other_members_list(%__MODULE__{all: all}) :: [pid] do
    PidSet.delete(all, self) |> PidSet.to_list
  end

  defun put_leader(m :: t, leader_or_nil :: nil | pid) :: t do
    # when resetting `leader`, `pending_leader_change field` should be discarded (if any)
    %__MODULE__{m | leader: leader_or_nil, pending_leader_change: nil}
  end

  defun start_adding_follower(%__MODULE__{all: all} = m,
                              {_term, _index, :add_follower, new_follower} = entry) :: R.t(t) do
    reject_if_change_ongoing(m, fn ->
      if PidSet.member?(all, new_follower) do
        {:error, :already_joined}
      else
        %__MODULE__{m | all: PidSet.put(all, new_follower), uncommitted_membership_change: entry} |> R.pure
      end
    end)
  end

  defun start_removing_follower(%__MODULE__{all: all} = m,
                                {_term, _index, :remove_follower, old_follower} = entry) :: R.t(t) do
    if old_follower == self do
      {:error, :cannot_remove_leader}
    else
      reject_if_change_ongoing(m, fn ->
        if PidSet.member?(all, old_follower) do
          %__MODULE__{m | all: PidSet.delete(all, old_follower), uncommitted_membership_change: entry} |> R.pure
        else
          {:error, :not_member}
        end
      end)
    end
  end

  defun start_replacing_leader(%__MODULE__{all: all} = m, new_leader :: pid) :: R.t(t) do
    if new_leader == self do
      {:error, :already_leader}
    else
      reject_if_change_ongoing(m, fn ->
        if PidSet.member?(all, new_leader) do
          %__MODULE__{m | pending_leader_change: new_leader} |> R.pure
        else
          {:error, :not_member}
        end
      end)
    end
  end

  defunp reject_if_change_ongoing(%__MODULE__{uncommitted_membership_change: c1, pending_leader_change: c2}, f :: (() -> any)) :: any do
    if c1 do
      {:error, :uncommitted_membership_change}
    else
      if c2 do
        {:error, :pending_leader_change}
      else
        f.()
      end
    end
  end

  defun membership_change_committed(%__MODULE__{uncommitted_membership_change: change} = m, index :: LogIndex.t) :: t do
    case change do
      {_, i, _, _} when i <= index -> %__MODULE__{m | uncommitted_membership_change: nil}
      _                            -> m
    end
  end
end
