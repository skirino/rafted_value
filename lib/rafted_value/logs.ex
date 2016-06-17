use Croma
alias Croma.TypeGen, as: TG
alias Croma.Result, as: R

defmodule RaftedValue.LogEntry do
  alias RaftedValue.{TermNumber, LogIndex}
  @type t :: {TermNumber.t, LogIndex.t, :leader_elected , pid}
           | {TermNumber.t, LogIndex.t, :add_follower   , pid}
           | {TermNumber.t, LogIndex.t, :remove_follower, pid}
           | {TermNumber.t, LogIndex.t, :command        , {GenServer.from, any, reference}}

  defun validate(v :: any) :: R.t(t) do
    {_, _, _, _} = t -> {:ok, t}
    _                -> {:error, {:invalid_value, [__MODULE__]}}
  end
end

defmodule RaftedValue.LogsMap do
  alias RaftedValue.LogIndex
  use Croma.SubtypeOfMap, key_module: LogIndex, value_module: Croma.Tuple
end

defmodule RaftedValue.Logs do
  alias RaftedValue.{LogIndex, LogEntry, LogInfo, LogsMap, FollowerIndices, TermNumber, PidSet, Members, Config}
  alias RaftedValue.RPC.AppendEntriesRequest

  use Croma.Struct, fields: [
    map:         LogsMap,
    i_min:       LogIndex,
    i_max:       LogIndex,
    i_committed: LogIndex,                    # this field represents also "applied index" as committed entry is immediately applied
    followers:   TG.nilable(FollowerIndices),
  ]

  #
  # for leader
  #
  defun new_for_lonely_leader :: t do
    first_entry = {0, 1, :leader_elected, self}
    %__MODULE__{map: %{1 => first_entry}, i_min: 1, i_max: 1, i_committed: 1, followers: %{}}
  end

  defun commit_to_latest(%__MODULE__{map: map, i_max: i_max, i_committed: i_c} = logs, config :: Config.t) :: {t, [LogEntry.t]} do
    new_logs = %__MODULE__{logs | i_committed: i_max} |> truncate_old_logs(config)
    applicable_entries = slice_entries(map, i_c + 1, i_max)
    {new_logs, applicable_entries}
  end

  defun make_append_entries_req(%__MODULE__{map: m, i_min: i_min, i_max: i_max, i_committed: i_c, followers: followers} = logs,
                                term         :: TermNumber.t,
                                follower_pid :: pid) :: {:ok, AppendEntriesRequest.t} | {:too_old, t} | :error do
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
            term:            term,
            leader_pid:      self,
            prev_log:        {term_prev, i_prev},
            entries:         slice_entries(m, i_next, i_max),
            i_leader_commit: i_c,
          } |> R.pure
        end
    end
  end

  defun set_follower_index(%__MODULE__{map: map, i_committed: old_i_committed, followers: followers} = logs,
                           %Members{all: members_set},
                           current_term :: TermNumber.t,
                           follower_pid :: pid,
                           i_replicated :: LogIndex.t,
                           config       :: Config.t) :: {t, [LogEntry.t]} do
    new_followers =
      Map.put(followers, follower_pid, {i_replicated + 1, i_replicated})
      |> Map.take(PidSet.delete(members_set, self) |> PidSet.to_list) # in passing we remove outdated entries in `new_followers`
    new_logs      = %__MODULE__{logs | followers: new_followers} |> update_commit_index(current_term, members_set, config)
    applicable_entries = slice_entries(map, old_i_committed + 1, new_logs.i_committed)
    {new_logs, applicable_entries}
  end

  defunp update_commit_index(%__MODULE__{map: map, i_max: i_max, i_committed: i_c, followers: followers} = logs,
                             current_term :: TermNumber.t,
                             members_set  :: PidSet.t,
                             config       :: Config.t) :: t do
    uncommitted_entries_reversed = slice_entries(map, i_c + 1, i_max) |> Enum.reverse
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
      {entry, _, _} -> %__MODULE__{logs | i_committed: elem(entry, 1)} |> truncate_old_logs(config)
    end
  end

  defunp can_commit?({term, index, _, _} :: LogEntry.t,
                     members_set         :: PidSet.t,
                     current_term        :: TermNumber.t,
                     followers           :: FollowerIndices.t) :: boolean do
    if term == current_term do
      follower_pids = members_set |> PidSet.delete(self) |> PidSet.to_list
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

  defun add_entry(%__MODULE__{map: map, i_max: i_max} = logs, config :: Config.t, f :: (LogIndex.t -> LogEntry.t)) :: t do
    i = i_max + 1
    entry = f.(i)
    %__MODULE__{logs | map: Map.put(map, i, entry), i_max: i}
    |> truncate_old_logs(config)
  end

  defun elected_leader(%__MODULE__{i_max: i_max} = logs, members :: Members.t, term :: TermNumber.t, config :: Config.t) :: t do
    follower_index_pair = {i_max + 1, 0}
    followers =
      Members.other_members_list(members)
      |> Enum.into(%{}, fn follower -> {follower, follower_index_pair} end)
    %__MODULE__{logs | followers: followers}
    |> add_entry(config, fn i -> {term, i, :leader_elected, self} end)
  end

  defun prepare_to_add_follower(%__MODULE__{i_max: i_max, followers: followers} = logs,
                                term         :: TermNumber.t,
                                new_follower :: pid,
                                config       :: Config.t) :: {t, LogEntry.t} do
    new_logs1 = %__MODULE__{logs | followers: Map.put(followers, new_follower, {i_max + 1, 0})}
    new_logs2 = add_entry(new_logs1, config, fn i -> {term, i, :add_follower, new_follower} end)
    {new_logs2, new_logs2.map[new_logs2.i_max]}
  end

  defun prepare_to_remove_follower(%__MODULE__{} = logs,
                                   term               :: TermNumber.t,
                                   follower_to_remove :: pid,
                                   config             :: Config.t) :: {t, LogEntry.t} do
    new_logs = add_entry(logs, config, fn i -> {term, i, :remove_follower, follower_to_remove} end)
    {new_logs, new_logs.map[new_logs.i_max]}
  end

  #
  # for non-leader
  #
  defun new_for_new_follower({_, i_committed, _, _} = last_committed_entry :: LogEntry.t) :: t do
    m = %{i_committed => last_committed_entry}
    %__MODULE__{map: m, i_min: i_committed, i_max: i_committed, i_committed: i_committed}
  end

  defun contain_given_prev_log?(%__MODULE__{map: m}, {term_prev, i_prev}) :: boolean do
    case m[i_prev] do
      nil   -> i_prev == 0 # first index is `1`
      entry -> elem(entry, 0) == term_prev
    end
  end

  defun append_entries(%__MODULE__{map: map, i_max: i_max, i_committed: old_i_committed} = logs,
                       %Members{all: members_set} = members,
                       entries         :: [LogEntry.t],
                       i_leader_commit :: LogIndex.t,
                       config          :: Config.t) :: {t, Members.t, [LogEntry.t]} do
    new_map = Enum.reduce(entries, map, fn(entry, m) ->
      Map.put(m, elem(entry, 1), entry)
    end)
    new_i_max = if Enum.empty?(entries), do: i_max, else: elem(List.last(entries), 1)
    new_logs = %__MODULE__{logs | map: new_map, i_max: new_i_max, i_committed: i_leader_commit} |> truncate_old_logs(config)
    applicable_entries = slice_entries(new_map, old_i_committed + 1, i_leader_commit)

    new_members_set = Enum.reduce(entries, members_set, &change_members/2)
    last_member_change_entry =
      slice_entries(new_map, i_max + 1, new_i_max)
      |> Enum.reverse
      |> Enum.find(fn {_, _, atom, _} -> atom in [:add_follower, :remove_follower] end)
    new_members =
      if last_member_change_entry do
        %Members{members | all: new_members_set, uncommitted_membership_change: last_member_change_entry}
      else
        %Members{members | all: new_members_set}
      end

    {new_logs, new_members, applicable_entries}
  end

  defun candidate_log_up_to_date?(%__MODULE__{map: m, i_max: i_max}, candidate_log_info :: LogInfo.t) :: boolean do
    {term, index, _, _} = m[i_max]
    {term, index} <= candidate_log_info
  end

  #
  # utilities
  #
  defun last_entry(%__MODULE__{map: m, i_max: i_max}) :: LogEntry.t do
    m[i_max]
  end

  defun last_committed_entry(%__MODULE__{map: m, i_committed: i_c}) :: LogEntry.t do
    m[i_c]
  end

  defunp change_members(entry :: LogEntry.t, members :: PidSet.t) :: PidSet.t do
    case entry do
      {_t, _i, :add_follower   , pid} -> PidSet.put(members, pid)
      {_t, _i, :remove_follower, pid} -> PidSet.delete(members, pid)
      _                               -> members
    end
  end

  defunp inverse_change_members(entry :: LogEntry.t, members :: PidSet.t) :: PidSet.t do
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

  defunp truncate_old_logs(%__MODULE__{map: map, i_min: i_min, i_committed: i_c} = logs,
                           %Config{max_retained_committed_logs: max_logs}) :: t do
    new_i_min = i_c - max_logs + 1
    if new_i_min > i_min do
      new_map = Enum.reduce(i_min .. new_i_min - 1, map, fn(i, m) -> Map.delete(m, i) end)
      %__MODULE__{logs | map: new_map, i_min: new_i_min}
    else
      logs
    end
  end
end
