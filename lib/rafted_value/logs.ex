use Croma
alias Croma.TypeGen, as: TG
alias Croma.Result, as: R

defmodule RaftedValue.LogsMap do
  alias RaftedValue.LogIndex
  use Croma.SubtypeOfMap, key_module: LogIndex, value_module: Croma.Tuple
end

defmodule RaftedValue.Logs do
  alias RaftedValue.{LogIndex, LogEntry, LogInfo, LogsMap, FollowerIndices, TermNumber, PidSet, Members, Persistence}
  alias RaftedValue.Persistence.SnapshotMetadata
  alias RaftedValue.RPC.AppendEntriesRequest

  use Croma.Struct, fields: [
    map:         LogsMap,
    i_min:       LogIndex,
    i_max:       LogIndex,
    i_committed: LogIndex,                    # this field represents also "applied index" as committed entry is immediately applied
    followers:   TG.nilable(FollowerIndices),
  ]

  defunp new_with_last_entry({_, index, _, _} = entry :: LogEntry.t) :: t do
    %__MODULE__{map: %{index => entry}, i_min: index, i_max: index, i_committed: index}
  end

  #
  # for leader
  #
  defun new_for_lonely_leader(last_committed_entry :: LogEntry.t, entries_to_append :: Enum.t(LogEntry.t)) :: t do
    logs = new_with_last_entry(last_committed_entry) |> Map.put(:followers, %{})
    Enum.reduce(entries_to_append, logs, fn({_, i, _, _} = entry, %__MODULE__{map: m} = l) ->
      %__MODULE__{l | map: Map.put(m, i, entry), i_max: i}
    end)
  end

  defun commit_to_latest(%__MODULE__{map: map, i_max: i_max, i_committed: i_c} = logs,
                         persistence :: nil | Persistence.t) :: {t, [LogEntry.t]} do
    new_logs = %__MODULE__{logs | i_committed: i_max} |> truncate_old_logs(persistence)
    entries_to_apply = slice_entries(map, i_c + 1, i_max)
    {new_logs, entries_to_apply}
  end

  defun make_append_entries_req(%__MODULE__{map: m, i_min: i_min, i_max: i_max, i_committed: i_c, followers: followers} = logs,
                                term         :: TermNumber.t,
                                follower_pid :: pid,
                                now          :: integer) :: {:ok, AppendEntriesRequest.t} | {:too_old, t} | :error do
    case followers[follower_pid] do
      nil         -> :error
      {i_next, _} ->
        if i_next < i_min do
          # this follower lags too behind (necessary logs are already discarded) => send whole data as snapshot
          new_followers = Map.put(followers, follower_pid, {i_max + 1, 0})
          {:too_old, %__MODULE__{logs | followers: new_followers}}
        else
          i_prev = i_next - 1
          term_prev =
            case m[i_prev] do
              nil   -> 0
              entry -> elem(entry, 0)
            end
          %AppendEntriesRequest{
            term:             term,
            leader_pid:       self(),
            prev_log:         {term_prev, i_prev},
            entries:          slice_entries(m, i_next, i_max),
            i_leader_commit:  i_c,
            leader_timestamp: now,
          } |> R.pure()
        end
    end
  end

  defun set_follower_index_as_snapshot_last_index(%__MODULE__{followers: followers} = logs,
                                                  %Members{all: members_set},
                                                  follower_pid :: pid,
                                                  %SnapshotMetadata{last_committed_index: index}) :: t do
    new_followers =
      Map.put(followers, follower_pid, {index + 1, index})
      |> Map.take(PidSet.delete(members_set, self()) |> PidSet.to_list()) # in passing we remove outdated entries in `new_followers`
    %__MODULE__{logs | followers: new_followers}
  end

  defun set_follower_index(%__MODULE__{map: map, i_committed: old_i_committed, followers: followers} = logs,
                           %Members{all: members_set},
                           current_term :: TermNumber.t,
                           follower_pid :: pid,
                           i_replicated :: LogIndex.t,
                           persistence  :: nil | Persistence.t) :: {t, [LogEntry.t]} do
    new_followers =
      Map.put(followers, follower_pid, {i_replicated + 1, i_replicated})
      |> Map.take(PidSet.delete(members_set, self()) |> PidSet.to_list()) # in passing we remove outdated entries in `new_followers`
    new_logs = %__MODULE__{logs | followers: new_followers} |> update_commit_index(current_term, members_set, persistence)
    entries_to_apply = slice_entries(map, old_i_committed + 1, new_logs.i_committed)
    {new_logs, entries_to_apply}
  end

  defunp update_commit_index(%__MODULE__{map: map, i_max: i_max, i_committed: i_c, followers: followers} = logs,
                             current_term :: TermNumber.t,
                             members_set  :: PidSet.t,
                             persistence  :: nil | Persistence.t) :: t do
    uncommitted_entries_reversed = slice_entries(map, i_c + 1, i_max) |> Enum.reverse()
    last_commitable_entry_tuple =
      Stream.scan(uncommitted_entries_reversed, {nil, nil, members_set}, fn(entry, {_, _, members_for_this_entry}) ->
        # Consensus group members change on :add_follower/:remove_follower log entries;
        # this means that "majority" changes before/after these entries.
        # Assuming that `members_set` parameter reflects all existing log entries up to `i_max`,
        # we restore consensus group members at each log entry.
        members_for_prev_entry = inverse_change_members(entry, members_for_this_entry)
        {entry, members_for_this_entry, members_for_prev_entry}
      end)
      |> Enum.find(fn {entry, set, _} ->
        can_commit?(entry, set, current_term, followers)
      end)
    case last_commitable_entry_tuple do
      nil           -> logs
      {entry, _, _} -> %__MODULE__{logs | i_committed: elem(entry, 1)} |> truncate_old_logs(persistence)
    end
  end

  defunp can_commit?({term, index, _, _} :: LogEntry.t,
                     members_set         :: PidSet.t,
                     current_term        :: TermNumber.t,
                     followers           :: FollowerIndices.t) :: boolean do
    if term == current_term do
      follower_pids = members_set |> PidSet.delete(self()) |> PidSet.to_list()
      n_necessary_followers = div(length(follower_pids) + 1, 2)
      n_uptodate_followers = Enum.count(follower_pids, fn f ->
        case followers[f] do
          {_, i} -> index <= i
          nil    -> false
        end
      end)
      n_necessary_followers <= n_uptodate_followers
    else
      false
    end
  end

  defun decrement_next_index_of_follower(%__MODULE__{followers: followers} = logs,
                                         follower_pid :: pid) :: t do
    case followers[follower_pid] do
      nil                            -> logs # from already removed follower
      {i_next_current, i_replicated} ->
        i_next_decremented = i_next_current - 1
        new_followers = Map.put(followers, follower_pid, {i_next_decremented, i_replicated})
        %__MODULE__{logs | followers: new_followers}
    end
  end

  defun add_entry(%__MODULE__{map: map, i_max: i_max} = logs,
                  persistence :: nil | Persistence.t,
                  f           :: (LogIndex.t -> LogEntry.t)) :: {t, LogEntry.t} do
    i = i_max + 1
    entry = f.(i)
    new_logs =
      %__MODULE__{logs | map: Map.put(map, i, entry), i_max: i}
      |> truncate_old_logs(persistence)
    {new_logs, entry}
  end

  defun add_entry_on_elected_leader(%__MODULE__{i_max: i_max} = logs,
                                    members     :: Members.t,
                                    term        :: TermNumber.t,
                                    persistence :: nil | Persistence.t) :: {t, LogEntry.t} do
    follower_index_pair = {i_max + 1, 0}
    followers =
      Members.other_members_list(members)
      |> Map.new(fn follower -> {follower, follower_index_pair} end)
    %__MODULE__{logs | followers: followers}
    |> add_entry(persistence, fn i -> {term, i, :leader_elected, self()} end)
  end

  defun add_entry_on_restored_from_files(logs :: t, term :: TermNumber.t) :: {t, LogEntry.t} do
    # We don't have to truncate the log entries here since it's right after recovery from disk snapshot
    add_entry(logs, nil, fn i -> {term, i, :restore_from_files, self()} end)
  end

  defun add_entry_on_add_follower(%__MODULE__{i_max: i_max, followers: followers} = logs,
                                  term         :: TermNumber.t,
                                  new_follower :: pid,
                                  persistence  :: nil | Persistence.t) :: {t, LogEntry.t} do
    %__MODULE__{logs | followers: Map.put(followers, new_follower, {i_max + 1, 0})}
    |> add_entry(persistence, fn i -> {term, i, :add_follower, new_follower} end)
  end

  defun add_entry_on_remove_follower(%__MODULE__{} = logs,
                                     term               :: TermNumber.t,
                                     follower_to_remove :: pid,
                                     persistence        :: nil | Persistence.t) :: {t, LogEntry.t} do
    add_entry(logs, persistence, fn i -> {term, i, :remove_follower, follower_to_remove} end)
  end

  #
  # for non-leader
  #
  defun new_for_new_follower(last_committed_entry :: LogEntry.t) :: t do
    new_with_last_entry(last_committed_entry)
  end

  defun contain_given_prev_log?(%__MODULE__{map: m}, {term_prev, i_prev}) :: boolean do
    case m[i_prev] do
      nil   -> i_prev == 0 # first index is `1`
      entry -> elem(entry, 0) == term_prev
    end
  end

  defun append_entries(%__MODULE__{map: map, i_max: i_max, i_committed: old_i_committed} = logs,
                       members         :: Members.t,
                       entries         :: [LogEntry.t],
                       i_leader_commit :: LogIndex.t,
                       persistence     :: nil | Persistence.t) :: {t, Members.t, [LogEntry.t], [LogEntry.t]} do
    {new_map, entries_to_persist_reversed} =
      Enum.reduce(entries, {map, []}, fn({_, i, _, _} = e, {m, acc}) ->
        case m[i] do
          ^e                -> {m, acc}
          _nil_or_different -> {Map.put(m, i, e), [e | acc]}
        end
      end)
    new_i_max          = if Enum.empty?(entries), do: i_max, else: max(i_max, elem(List.last(entries), 1))
    new_i_committed    = max(old_i_committed, i_leader_commit)
    new_logs           = %__MODULE__{logs | map: new_map, i_max: new_i_max, i_committed: new_i_committed} |> truncate_old_logs(persistence)
    entries_to_apply   = slice_entries(new_map, old_i_committed + 1, new_i_committed)
    entries_to_persist = Enum.reverse(entries_to_persist_reversed)
    new_members        = Enum.reduce(entries_to_persist, members, &change_members/2)
    {new_logs, new_members, entries_to_apply, entries_to_persist}
  end

  defun candidate_log_up_to_date?(%__MODULE__{map: m, i_max: i_max}, candidate_log_info :: LogInfo.t) :: boolean do
    {term, index, _, _} = m[i_max]
    {term, index} <= candidate_log_info
  end

  #
  # utilities
  #
  defun last_entry(%__MODULE__{map: m, i_max: i_max}) :: LogEntry.t do
    Map.fetch!(m, i_max)
  end

  defun last_committed_entry(%__MODULE__{map: m, i_committed: i_c}) :: LogEntry.t do
    Map.fetch!(m, i_c)
  end

  defunp change_members(entry :: LogEntry.t, %Members{all: set} = members) :: Members.t do
    case entry do
      {_t, _i, :add_follower      , pid} -> %Members{members | all: PidSet.put(set, pid)         , uncommitted_membership_change: entry}
      {_t, _i, :remove_follower   , pid} -> %Members{members | all: PidSet.delete(set, pid)      , uncommitted_membership_change: entry}
      {_t, _i, :restore_from_files, pid} -> %Members{members | all: PidSet.put(PidSet.new(), pid), uncommitted_membership_change: nil  }
      _                                  -> members
    end
  end

  defunp inverse_change_members(entry :: LogEntry.t, members :: PidSet.t) :: PidSet.t do
    # We don't have to take `:restore_from_files` into consideration here because it is immediately committed by the lonely leader
    case entry do
      {_t, _i, :add_follower   , pid} -> PidSet.delete(members, pid)
      {_t, _i, :remove_follower, pid} -> PidSet.put(members, pid)
      _                               -> members
    end
  end

  defunp slice_entries(map :: LogsMap.t, i1 :: LogIndex.t, i2 :: LogIndex.t) :: [LogEntry.t] do
    if i1 <= i2 do
      Enum.map(i1 .. i2, fn i -> map[i] end)
    else
      []
    end
  end

  @extra_log_entries_kept_in_memory (if Mix.env() == :test, do: 50, else: 100)

  defunp truncate_old_logs(%__MODULE__{map: map, i_min: i_min, i_committed: i_c} = logs,
                           persistence_or_nil :: nil | Persistence.t) :: t do
    index_removable_upto =
      case persistence_or_nil do
        %Persistence{latest_snapshot_metadata: nil} ->
          0 # persisting logs but no snapshot has been created on disk => during initialization and should not truncate log entries
        %Persistence{latest_snapshot_metadata: %SnapshotMetadata{last_committed_index: i}} ->
          i # snapshot which reflects log entries upto `i` => upto `i` can be discarded
        nil ->
          i_c # purely in-memory setup, log entries can be discarded immediately after they are committed
      end
    # We don't use `followers` field to compute threshold index to discard; use fixed value (`100`) to keep extra log entries instead,
    # because `followers` field is available only to leader processes.
    # If a follower lags too behind then leader gives up log shipping and sends a snapshot, so it's OK.
    case index_removable_upto - @extra_log_entries_kept_in_memory + 1 do
      new_i_min when new_i_min > i_min -> %__MODULE__{logs | map: Map.drop(map, i_min .. new_i_min - 1), i_min: new_i_min}
      _                                -> logs
    end
  end
end
