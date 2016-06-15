use Croma

defmodule RaftedValue.Server do
  @moduledoc """
  TODO: Write something
  """

  #
  # Implementation notes
  #
  # ## events
  #
  # - async
  #   - defined in Raft (all contains `term`)
  #     - AppendEntriesRequest
  #     - AppendEntriesResponse
  #     - RequestVoteRequest
  #     - RequestVoteResponse
  #     - InstallSnapshot
  #   - others
  #     - :heartbeat_timeout
  #     - :election_timeout (also used as `TimeoutNow` when replacing leader)
  #     - :leave_and_stop
  #     - :remove_follower_completed
  #     - :cannot_reach_quorum
  # - sync
  #   - {:command, arg, cmd_id}
  #   - {:add_follower, pid}
  #   - {:remove_follower, pid}
  #   - {:replace_leader, new_leader}
  #

  alias RaftedValue.{TermNumber, PidSet, Members, Leadership, Election, Logs, CommandResults, Config}
  alias RaftedValue.RPC.{
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
    InstallSnapshot,
  }

  defmodule State do
    use Croma.Struct, fields: [
      members:         Members,
      current_term:    TermNumber,
      leadership:      Leadership,
      election:        Election,
      logs:            Logs,
      data:            Croma.Any,      # replicated using raft logs (i.e. reproducible from logs)
      command_results: CommandResults, # replicated using raft logs (i.e. reproducible from logs)
      config:          Config,
    ]
  end

  @behaviour :gen_fsm

  defmacrop same_fsm_state(state_data) do
    {state_name, _arity} = __CALLER__.function
    quote bind_quoted: [state_name: state_name, state_data: state_data] do
      {:next_state, state_name, state_data}
    end
  end

  defmacrop same_fsm_state_reply(state_data, reply) do
    {state_name, _arity} = __CALLER__.function
    quote bind_quoted: [state_name: state_name, state_data: state_data, reply: reply] do
      {:reply, reply, state_name, state_data}
    end
  end

  defp next_state(state_data, state_name) do
    {:next_state, state_name, state_data}
  end

  #
  # initialization
  #
  def init({:create_new_consensus_group, config}) do
    data        = config.data_ops_module.new
    logs        = Logs.new_for_lonely_leader
    members     = Members.new_for_lonely_leader
    leadership  = Leadership.new_for_leader(config)
    election    = Election.new_for_leader
    cmd_results = CommandResults.new
    state = %State{members: members, current_term: 0, leadership: leadership, election: election, logs: logs, data: data, command_results: cmd_results, config: config}
    {:ok, :leader, state}
  end
  def init({:join_existing_consensus_group, known_members}) do
    %InstallSnapshot{members: members, term: term, last_committed_entry: last_entry, data: data, command_results: command_results, config: config} =
      call_add_server(known_members)
    logs = Logs.new_for_new_follower(last_entry)
    election = Election.new_for_follower(config)
    state = %State{members: members, current_term: term, leadership: Leadership.new, election: election, logs: logs, data: data, command_results: command_results, config: config}
    {:ok, :follower, state}
  end

  defunp call_add_server(known_members :: [GenServer.server]) :: InstallSnapshot.t do
    []                  -> raise "no leader found"
    [m | known_members] ->
      case call_add_server_one(m) do
        {:ok, suc}                      -> suc
        {:error, {:not_leader, nil}}    -> call_add_server(known_members)
        {:error, {:not_leader, leader}} -> call_add_server([leader | Enum.reject(known_members, &(&1 == leader))])
        {:error, :noproc}               -> call_add_server(known_members)
        # {:error, :uncommitted_membership_change} results in an error
      end
  end

  defunp call_add_server_one(maybe_leader :: GenServer.server) :: Croma.Result.t(InstallSnapshot.t) do
    try do
      :gen_fsm.sync_send_event(maybe_leader, {:add_follower, self})
    catch
      :exit, {:noproc, _} -> {:error, :noproc}
    end
  end

  #
  # leader state
  #
  def leader(%AppendEntriesResponse{from: from, success: success, i_replicated: i_replicated} = rpc,
             %State{members: members, current_term: current_term, leadership: leadership, logs: logs, config: config} = state) do
    become_follower_if_new_term_started(rpc, state, fn ->
      new_leadership = Leadership.follower_responded(leadership, members, from, config)
      if success do
        {new_logs, applicable_entries} = Logs.set_follower_index(logs, members, current_term, from, i_replicated, config)
        new_state1 = %State{state | leadership: new_leadership, logs: new_logs}
        new_state2 = Enum.reduce(applicable_entries, new_state1, &leader_apply_committed_log_entry/2)
        case members do
          %Members{pending_leader_change: ^from} ->
            if i_replicated == new_logs.i_max do
              # now we know that the follower `from`'s logs are up-to-date; send `:election_timeout` to make it a new leader
              send_event(new_state2, from, :election_timeout)
              convert_state_as_follower(new_state2, current_term) |> next_state(:follower)
            else
              # try again at next `AppendEntriesResponse`
              same_fsm_state(new_state2)
            end
          _ -> same_fsm_state(new_state2)
        end
      else
        # prev log from leader didn't match follower's => decrement "next index" for the follower and try to resend AppendEntries
        new_logs = Logs.decrement_next_index_of_follower(logs, from)
        %State{state | leadership: new_leadership, logs: new_logs}
        |> send_append_entries(from)
        |> same_fsm_state
      end
    end)
  end
  def leader(:heartbeat_timeout, state) do
    broadcast_append_entries(state) |> same_fsm_state
  end
  def leader(:cannot_reach_quorum, %State{current_term: term} = state) do
    convert_state_as_follower(state, term)
    |> next_state(:follower)
  end
  def leader(%RequestVoteRequest{} = rpc, state) do
    handle_request_vote_request(rpc, state, :leader)
  end
  def leader(%{__struct__: s} = rpc, state) when s == AppendEntriesRequest or s == RequestVoteResponse do
    become_follower_if_new_term_started(rpc, state, fn ->
      same_fsm_state(state) # neglect `AppendEntriesRequest`, `RequestVoteResponse` for this term / older term
    end)
  end
  def leader(_event, state) do
    same_fsm_state(state) # leader neglects `:election_timeout`, `:leave_and_stop`, `:remove_follower_completed`, `InstallSnapshot`
  end

  def leader({:command, arg, cmd_id}, from, %State{current_term: term, logs: logs, config: config} = state) do
    new_logs = Logs.add_entry(logs, config, fn index -> {term, index, :command, {from, arg, cmd_id}} end)
    %State{state | logs: new_logs}
    |> broadcast_append_entries
    |> same_fsm_state
  end
  def leader({:add_follower, new_follower},
             from,
             %State{members: members, current_term: term, logs: logs, config: config} = state) do
    {new_logs, add_follower_entry} = Logs.prepare_to_add_follower(logs, term, new_follower, config)
    case Members.start_adding_follower(members, add_follower_entry) do
      {:error, _} = e    -> same_fsm_state_reply(state, e)
      {:ok, new_members} ->
        reply(state, from, {:ok, make_install_snapshot(state)})
        %State{state | members: new_members, logs: new_logs}
        |> broadcast_append_entries
        |> same_fsm_state
    end
  end
  def leader({:remove_follower, old_follower},
             _from,
             %State{members: members, current_term: term, logs: logs, config: config} = state) do
    {new_logs, remove_follower_entry} = Logs.prepare_to_remove_follower(logs, term, old_follower, config)
    case Members.start_removing_follower(members, remove_follower_entry) do
      {:error, _} = e    -> same_fsm_state_reply(state, e)
      {:ok, new_members} ->
        %State{state | members: new_members, logs: new_logs}
        |> broadcast_append_entries
        |> same_fsm_state_reply(:ok)
    end
  end
  def leader({:replace_leader, new_leader},
             _from,
             %State{members: members} = state) do
    case Members.start_replacing_leader(members, new_leader) do
      {:ok, new_members} -> %State{state | members: new_members} |> same_fsm_state_reply(:ok)
      {:error, _} = e    -> same_fsm_state_reply(state, e)
    end
  end

  def become_leader(%State{members: members, current_term: term, logs: logs, config: config} = state) do
    new_logs = Logs.elected_leader(logs, members, term, config)
    %State{state | members: Members.put_leader(members, self), logs: new_logs}
    |> broadcast_append_entries
    |> next_state(:leader)
  end

  defunp broadcast_append_entries(%State{members: members, leadership: leadership, logs: logs, config: config} = state) :: State.t do
    followers = Members.other_members_list(members)
    if Enum.empty?(followers) do
      # When there's no other member in this consensus group, the leader won't receive AppendEntriesResponse;
      # here is the time to make decisions (solely by itself) by committing new entries.
      {new_logs, applicable_entries} = Logs.commit_to_latest(logs, config)
      new_leadership = Leadership.reset_quorum_timer(leadership, config) # quorum is reached by the leader itself
      new_state = %State{state | leadership: new_leadership, logs: new_logs}
      Enum.reduce(applicable_entries, new_state, &leader_apply_committed_log_entry/2)
    else
      Enum.reduce(followers, state, fn(follower, s) ->
        send_append_entries(s, follower)
      end)
    end
    |> reset_heartbeat_timer
  end

  defunp send_append_entries(%State{current_term: term, logs: logs} = state, follower :: pid) :: State.t do
    case Logs.make_append_entries_req(logs, term, follower) do
      {:ok, req} ->
        send_event(state, follower, req)
        state
      {:too_old, new_logs} ->
        new_state = %State{state | logs: new_logs} # reset follower's next index
        send_event(new_state, follower, make_install_snapshot(new_state))
        new_state
    end
  end

  defunp make_install_snapshot(%State{members: members, current_term: term, logs: logs, data: data, command_results: command_results, config: config}) :: InstallSnapshot.t do
    last_entry = Logs.last_committed_entry(logs)
    %InstallSnapshot{members: members, term: term, last_committed_entry: last_entry, data: data, command_results: command_results, config: config}
  end

  #
  # candidate state
  #
  def candidate(%AppendEntriesRequest{} = req, state) do
    handle_append_entries_request(req, state, :candidate)
  end
  def candidate(%AppendEntriesResponse{} = rpc, state) do
    become_follower_if_new_term_started(rpc, state, fn ->
      same_fsm_state(state) # neglect `AppendEntriesResponse` from this term / older term
    end)
  end
  def candidate(%RequestVoteRequest{} = rpc, state) do
    handle_request_vote_request(rpc, state, :candidate)
  end
  def candidate(%RequestVoteResponse{from: from, term: term, vote_granted: granted?} = rpc,
                %State{members: members, current_term: current_term, election: election} = state) do
    become_follower_if_new_term_started(rpc, state, fn ->
      if term < current_term or !granted? do
        same_fsm_state(state) # neglect `RequestVoteResponse` from older term
      else
        {new_election, majority?} = Election.gain_vote(election, members, from)
        new_state = %State{state | election: new_election}
        if majority? do
          become_leader(new_state)
        else
          same_fsm_state(new_state)
        end
      end
    end)
  end
  def candidate(:election_timeout, state) do
    become_candidate_and_start_new_election(state)
  end
  def candidate(_event, state) do
    same_fsm_state(state) # neglect `:heartbeat_timeout`, `:leave_and_stop`, `:remove_follower_completed`, `cannot_reach_quorum`, `InstallSnapshot`
  end

  def candidate(_event, _from, %State{members: members} = state) do
    # non-leader rejects synchronous events: `{:command, arg, cmd_id}`, `{:add_follower, pid}` and `{:remove_follower, pid}`
    same_fsm_state_reply(state, {:error, {:not_leader, members.leader}})
  end

  defp become_candidate_and_start_new_election(%State{members: members, current_term: term, election: election, config: config} = state) do
    new_members  = Members.put_leader(members, nil)
    new_election = Election.replace_for_candidate(election, config)
    new_state = %State{state | members: new_members, current_term: term + 1, election: new_election}
    broadcast_request_vote(new_state)
    next_state(new_state, :candidate)
  end

  defunp broadcast_request_vote(%State{members: members, current_term: term, logs: logs} = state) :: :ok do
    Members.other_members_list(members) |> Enum.each(fn member ->
      {last_log_term, last_log_index, _, _} = Logs.last_entry(logs)
      req = %RequestVoteRequest{term: term, candidate_pid: self, last_log: {last_log_term, last_log_index}}
      send_event(state, member, req)
    end)
  end

  #
  # follower state
  #
  def follower(%AppendEntriesRequest{} = req, state) do
    handle_append_entries_request(req, state, :follower)
  end
  def follower(%RequestVoteRequest{} = rpc, state) do
    handle_request_vote_request(rpc, state, :follower)
  end
  def follower(%{__struct__: s} = rpc, state) when s == AppendEntriesResponse or s == RequestVoteResponse do
    become_follower_if_new_term_started(rpc, state, fn ->
      same_fsm_state(state) # neglect `AppendEntriesResponse`, `RequestVoteResponse` from this term / older term
    end)
  end
  def follower(:election_timeout, state) do
    become_candidate_and_start_new_election(state)
  end
  def follower(:leave_and_stop, %State{members: members} = state) do
    new_state = cancel_election_timer(state)
    _ = :gen_fsm.sync_send_event(members.leader, {:remove_follower, self})
    same_fsm_state(new_state)
  end
  def follower(:remove_follower_completed, state) do
    {:stop, :normal, state}
  end
  def follower(%InstallSnapshot{members: members, term: term, last_committed_entry: last_entry, data: data, command_results: command_results} = rpc,
               state) do
    become_follower_if_new_term_started(rpc, state, fn ->
      logs = Logs.new_for_new_follower(last_entry)
      %State{state | members: members, current_term: term, logs: logs, data: data, command_results: command_results}
      |> reset_election_timer
      |> same_fsm_state
    end)
  end
  def follower(_event, state) do
    same_fsm_state(state) # neglect `:heartbeat_timeout`, `cannot_reach_quorum`,
  end

  def follower(_event, _from, %State{members: members} = state) do
    # non-leader rejects synchronous events: `{:command, arg, cmd_id}`, `{:add_follower, pid}` and `{:remove_follower, pid}`
    same_fsm_state_reply(state, {:error, {:not_leader, members.leader}})
  end

  defp become_follower_if_new_term_started(%{term: term} = rpc,
                                           %State{current_term: current_term} = state,
                                           else_fn) do
    if term > current_term do
      new_state = convert_state_as_follower(state, term)
      # process the given RPC message as a follower
      # (there are cases where `election.timer` started right above will be immediately resetted in `follower/2` but it's rare)
      follower(rpc, new_state)
    else
      else_fn.()
    end
  end

  defunp convert_state_as_follower(%State{members: members, leadership: leadership, election: election, config: config} = state,
                                   new_term :: TermNumber.t) :: State.t do
    new_members    = Members.put_leader(members, nil)
    new_leadership = Leadership.deactivate(leadership)
    new_election   = Election.replace_for_follower(election, config)
    %State{state | members: new_members, current_term: new_term, leadership: new_leadership, election: new_election}
  end

  #
  # common handler implementations
  #
  defp handle_append_entries_request(%AppendEntriesRequest{term: term, leader_pid: leader_pid, prev_log: prev_log,
                                                                     entries: entries, i_leader_commit: i_leader_commit},
                                     %State{members: members, current_term: current_term, logs: logs, config: config} = state,
                                     current_state_name) do
    reply_as_failure = fn larger_term ->
      send_event(state, leader_pid, %AppendEntriesResponse{from: self, term: larger_term, success: false})
    end

    if term < current_term do
      # AppendEntries from leader for older term => reject
      reply_as_failure.(current_term)
      next_state(state, current_state_name)
    else
      if Logs.contain_given_prev_log?(logs, prev_log) do
        {new_logs, new_members1, applicable_entries} = Logs.append_entries(logs, members, entries, i_leader_commit, config)
        new_members2 = Members.put_leader(new_members1, leader_pid)
        new_state1 = %State{state | members: new_members2, current_term: term, logs: new_logs}
        new_state2 = Enum.reduce(applicable_entries, new_state1, &nonleader_apply_committed_log_entry/2)
        reply = %AppendEntriesResponse{from: self, term: term, success: true, i_replicated: new_logs.i_max}
        send_event(new_state2, leader_pid, reply)
        new_state2
      else
        # this follower does not have `prev_log` => ask leader to resend older logs
        reply_as_failure.(term)
        new_members = Members.put_leader(members, leader_pid)
        %State{state | members: new_members, current_term: term}
      end
      |> reset_election_timer
      |> next_state(:follower)
    end
  end

  defp handle_request_vote_request(%RequestVoteRequest{term: term, candidate_pid: candidate, last_log: last_log} = rpc,
                                   %State{current_term: current_term, election: election, logs: logs, config: config} = state,
                                   current_state_name) do
    become_follower_if_new_term_started(rpc, state, fn ->
      grant_vote? = (
        term == current_term                   and # the case `term > current_term` is covered by `become_follower_if_new_term_started`
        election.voted_for in [nil, candidate] and
        Logs.candidate_log_up_to_date?(logs, last_log))
      response = %RequestVoteResponse{from: self, term: current_term, vote_granted: grant_vote?}
      send_event(state, candidate, response)
      if grant_vote? do
        %State{state | election: Election.vote_for(election, candidate, config)}
      else
        state
      end
      |> next_state(current_state_name)
    end)
  end

  #
  # other callbacks
  #
  def handle_event(_event, state_name, state) do
    next_state(state, state_name)
  end

  def handle_sync_event(_event, _from, state_name, %State{members: members} = state) do
    leader = members.leader
    members = PidSet.to_list(members.all)
    {:reply, {members, leader}, state_name, state}
  end

  def handle_info(_info, state_name, state) do
    next_state(state, state_name)
  end

  def terminate(_reason, _state_name, _state) do
    :ok
  end

  def code_change(_old, state_name, state, _extra) do
    {:ok, state_name, state}
  end

  #
  # utilities
  #
  defp send_event(%State{config: %Config{communication_module: mod}}, dest, event) do
    mod.send_event(dest, event)
  end

  defp reply(%State{config: %Config{communication_module: mod}}, from, reply) do
    mod.reply(from, reply)
  end

  defunp reset_heartbeat_timer(%State{leadership: leadership, config: config} = state) :: State.t do
    %State{state | leadership: Leadership.reset_heartbeat_timer(leadership, config)}
  end

  defunp reset_election_timer(%State{election: election, config: config} = state) :: State.t do
    %State{state | election: Election.reset_timer(election, config)}
  end

  defunp cancel_election_timer(%State{election: election} = state) :: State.t do
    %State{state | election: Election.cancel_timer(election)}
  end

  defunp leader_apply_committed_log_entry(entry :: LogEntry.t, %State{members: members, logs: logs} = state) :: State.t do
    case entry do
      {_term, _index, :command        , tuple        } -> run_command(state, tuple, true)
      {_term, _index, :leader_elected , _leader_pid  } -> state
      {_term, index , :add_follower   , _follower_pid} -> %State{state | members: Members.membership_change_committed(members, index)}
      {_term, index , :remove_follower, follower_pid } ->
        send_event(state, follower_pid, :remove_follower_completed)
        new_logs = Logs.forget_about_follower(logs, follower_pid)
        %State{state | members: Members.membership_change_committed(members, index), logs: new_logs}
    end
  end

  defunp nonleader_apply_committed_log_entry(entry :: LogEntry.t, %State{members: members} = state) :: State.t do
    case entry do
      {_term, _index, :command        , tuple        } -> run_command(state, tuple, false)
      {_term, _index, :leader_elected , _leader_pid  } -> state
      {_term, index , :add_follower   , _follower_pid} -> %State{state | members: Members.membership_change_committed(members, index)}
      {_term, index , :remove_follower, _follower_pid} -> %State{state | members: Members.membership_change_committed(members, index)}
    end
  end

  defp run_command(%State{data: data, command_results: command_results, config: config} = state, {client, arg, cmd_id}, leader?) do
    reply_if_leader = fn result ->
      if leader? do
        reply(state, client, {:ok, result})
      end
    end

    case CommandResults.fetch(command_results, cmd_id) do
      {:ok, result} ->
        # this command is already executed => don't execute command twice and just return
        reply_if_leader.(result)
        state
      :error ->
        %Config{data_ops_module: mod, max_retained_command_results: max} = config
        {result, new_data} = mod.command(data, arg)
        new_command_results = CommandResults.put(command_results, cmd_id, result, max)
        reply_if_leader.(result)
        %State{state | data: new_data, command_results: new_command_results}
    end
  end
end
