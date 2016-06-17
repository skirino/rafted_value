defmodule RaftedValueTest do
  use ExUnit.Case
  alias RaftedValue.PidSet

  defmodule I do
    @behaviour RaftedValue.DataOps
    @type t :: integer
    def new, do: 0
    def command(i, :get     ), do: {i, i    }
    def command(i, {:set, j}), do: {i, j    }
    def command(i, :inc     ), do: {i, i + 1}
  end

  @conf RaftedValue.make_config(I, [
    max_retained_committed_logs:  10,
    max_retained_command_results: 10,
    heartbeat_timeout:            100,
    election_timeout:             500,
  ])

  @t_max_election_timeout @conf.election_timeout * 2

  defp assert_equal_as_set(set1, set2) do
    assert Enum.sort(set1) == Enum.sort(set2)
  end

  defp wait_until_member_change_completes(leader) do
    :timer.sleep(10)
    {:leader, state} = :sys.get_state(leader)
    members = state.members
    if members.uncommitted_membership_change && members.pending_leader_change do
      wait_until_member_change_completes(leader)
    else
      :ok
    end
  end

  defp wait_until_state_name_changes(member, state_name) do
    :timer.sleep(10)
    case :sys.get_state(member) do
      {^state_name, _} -> :ok
      _                -> wait_until_state_name_changes(member, state_name)
    end
  end

  defp add_follower(leader, name \\ nil) do
    {:ok, follower} = RaftedValue.start_link({:join_existing_consensus_group, [leader]}, name)
    wait_until_member_change_completes(leader)
    follower
  end

  defp make_cluster(n_follower, config \\ @conf) do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, config})
    followers = Enum.map(1 .. n_follower, fn _ -> add_follower(leader) end)
    {leader, followers}
  end

  defp wait_until_someone_elected_leader(members) do
    :timer.sleep(10)
    leader = Enum.find(members, fn m ->
      match?({:leader, _}, :sys.get_state(m))
    end)
    if leader do
      leader
    else
      wait_until_someone_elected_leader(members)
    end
  end

  def simulate_send_sync_event(dest, event) do
    ref = make_ref
    send(dest, {:"$gen_sync_event", {self, ref}, event})
    ref
  end

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  test "should appropriately start/add/remove/stop server" do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf}, :foo)
    assert Process.whereis(:foo) == leader
    follower1 = add_follower(leader, :bar)
    assert Process.whereis(:bar) == follower1
    {:ok, follower2} = RaftedValue.start_link({:join_existing_consensus_group, [follower1, leader]}, :baz)
    wait_until_member_change_completes(leader)
    assert Process.whereis(:baz) == follower2

    assert RaftedValue.remove_follower(leader, follower1) == :ok
    wait_until_member_change_completes(leader)
    assert_receive({:EXIT, ^follower1, :normal})
    :timer.sleep(100) # for coverage: wait until :remove_follower log entry is applied in follwer2

    assert RaftedValue.remove_follower(leader, follower2) == :ok
    wait_until_member_change_completes(leader)
    assert_receive({:EXIT, ^follower2, :normal})

    assert :gen_fsm.stop(leader) == :ok
    refute Process.alive?(leader)
  end

  test "should not concurrently execute multiple membership changes" do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf})
    _follower1 = add_follower(leader)

    ref1 = simulate_send_sync_event(leader, {:add_follower, self})
    ref2 = simulate_send_sync_event(leader, {:add_follower, self})
    assert_receive({^ref1, {:ok, %RaftedValue.RPC.InstallSnapshot{}}})
    assert_receive({^ref2, {:error, :uncommitted_membership_change}})

    wait_until_member_change_completes(leader)

    ref1 = simulate_send_sync_event(leader, {:remove_follower, self})
    ref2 = simulate_send_sync_event(leader, {:remove_follower, self})
    assert_receive({^ref1, :ok})
    assert_receive({^ref2, {:error, :uncommitted_membership_change}})

    wait_until_member_change_completes(leader)
  end

  test "should report error when trying to add already joined member" do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf})
    follower1 = add_follower(leader)
    assert :gen_fsm.sync_send_event(leader, {:add_follower, follower1}) == {:error, :already_joined}
  end

  test "should report error when trying to remove non member" do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf})
    assert RaftedValue.remove_follower(leader, self) == {:error, :not_member}
  end

  test "start_link_and_join_consensus_group should return error and the process should die when no leader found" do
    {:error, _} = RaftedValue.start_link({:join_existing_consensus_group, [:unknown, :member]})
    assert_receive({:EXIT, _pid, _})
  end

  test "should refuse to remove healthy follower if it breaks the current quorum" do
    {leader, [follower1, follower2]} = make_cluster(2)
    assert :gen_fsm.stop(follower1) == :ok
    :timer.sleep(@t_max_election_timeout)
    assert RaftedValue.remove_follower(leader, follower2) == {:error, :will_break_quorum}
  end

  test "replace_leader should eventually replace leader" do
    {leader, [follower1, follower2]} = make_cluster(2)

    assert RaftedValue.replace_leader(leader, leader) == {:error, :already_leader}

    assert RaftedValue.replace_leader(leader, follower1) == :ok
    wait_until_state_name_changes(leader, :follower)
    wait_until_state_name_changes(follower1, :leader)
    assert RaftedValue.run_command(leader   , :inc) == {:error, {:not_leader, follower1}}
    assert RaftedValue.run_command(follower1, :inc) == {:ok, 0}

    assert RaftedValue.replace_leader(follower1, follower2) == :ok
    wait_until_state_name_changes(follower1, :follower)
    wait_until_state_name_changes(follower2, :leader)
    assert RaftedValue.run_command(follower1, :inc) == {:error, {:not_leader, follower2}}
    assert RaftedValue.run_command(follower2, :inc) == {:ok, 1}
  end

  test "later replace_leader operation should override previous one" do
    {leader, [follower1, follower2]} = make_cluster(2)

    ref1 = simulate_send_sync_event(leader, {:replace_leader, follower1})
    ref2 = simulate_send_sync_event(leader, {:replace_leader, follower2})
    assert_receive({^ref1, :ok})
    assert_receive({^ref2, :ok})

    wait_until_state_name_changes(leader, :follower)
    wait_until_state_name_changes(follower2, :leader)
    {:follower, _} = :sys.get_state(follower1)
  end

  test "replace_leader should cancel previous call by passing nil" do
    {leader, [follower1, follower2]} = make_cluster(2)

    ref1 = simulate_send_sync_event(leader, {:replace_leader, follower1})
    ref2 = simulate_send_sync_event(leader, {:replace_leader, nil})
    assert_receive({^ref1, :ok})
    assert_receive({^ref2, :ok})

    :timer.sleep(10)
    {:leader  , _} = :sys.get_state(leader)
    {:follower, _} = :sys.get_state(follower1)
    {:follower, _} = :sys.get_state(follower2)
  end

  test "replace_leader should reject change to unhealthy follower" do
    {leader, [follower1, _]} = make_cluster(2)
    assert :gen_fsm.stop(follower1) == :ok
    :timer.sleep(@t_max_election_timeout)
    assert RaftedValue.replace_leader(leader, follower1) == {:error, :new_leader_unresponsive}
  end

  test "leader should respond to client requests" do
    {leader, [follower1, follower2]} = make_cluster(2)

    assert RaftedValue.run_command(leader, :inc     ) == {:ok, 0}
    assert RaftedValue.run_command(leader, :inc     ) == {:ok, 1}
    assert RaftedValue.run_command(leader, :inc     ) == {:ok, 2}
    assert RaftedValue.run_command(leader, :inc     ) == {:ok, 3}
    assert RaftedValue.run_command(leader, :get     ) == {:ok, 4}
    assert RaftedValue.run_command(leader, {:set, 1}) == {:ok, 4}
    assert RaftedValue.run_command(leader, :get     ) == {:ok, 1}
    assert RaftedValue.run_command(leader, :inc     ) == {:ok, 1}
    assert RaftedValue.run_command(leader, :inc     ) == {:ok, 2}
    assert RaftedValue.run_command(leader, :get     ) == {:ok, 3}

    assert RaftedValue.run_command(follower1, :get) == {:error, {:not_leader, leader}}
    assert RaftedValue.run_command(follower2, :get) == {:error, {:not_leader, leader}}
  end

  test "should not execute the same client requests with identical reference multiple times" do
    {leader, _} = make_cluster(2)

    assert RaftedValue.run_command(leader, :get     ) == {:ok, 0}
    assert RaftedValue.run_command(leader, {:set, 1}) == {:ok, 0}
    assert RaftedValue.run_command(leader, {:set, 2}) == {:ok, 1}
    assert RaftedValue.run_command(leader, :get     ) == {:ok, 2}

    ref = make_ref
    assert RaftedValue.run_command(leader, :inc, 5000, ref) == {:ok, 2}
    assert RaftedValue.run_command(leader, :inc, 5000, ref) == {:ok, 2}
    assert RaftedValue.run_command(leader, :inc, 5000, ref) == {:ok, 2}

    assert RaftedValue.run_command(leader, :get) == {:ok, 3}
  end

  test "1-member cluster should immediately respond to client requests" do
    {leader, _} = make_cluster(0)
    assert RaftedValue.run_command(leader, :inc) == {:ok, 0}
    :timer.sleep(@conf.election_timeout) # should not step down after election timeout
    assert RaftedValue.run_command(leader, :inc) == {:ok, 1}
    :timer.sleep(@conf.election_timeout)
    assert RaftedValue.run_command(leader, :inc) == {:ok, 2}
  end

  test "3,4,5,6,7 member cluster should tolerate up to 1,1,2,2,3 follower failure" do
    [3, 4, 5, 6, 7] |> Enum.each(fn n_members ->
      {leader, followers} = make_cluster(n_members - 1)
      assert RaftedValue.run_command(leader, {:set, 1}) == {:ok, 0}
      assert RaftedValue.run_command(leader, :get     ) == {:ok, 1}

      followers_failing = Enum.take_random(followers, div(n_members - 1, 2))
      Enum.each(followers_failing, fn f ->
        assert :gen_fsm.stop(f) == :ok
        assert_received({:EXIT, ^f, :normal})
        assert RaftedValue.run_command(leader, :get) == {:ok, 1}
      end)

      # leader should not step down as long as it can access majority of members
      {:leader, _} = :sys.get_state(leader)
      :timer.sleep(@t_max_election_timeout)
      {:leader, _} = :sys.get_state(leader)

      follower_threshold = Enum.random(followers -- followers_failing)
      assert :gen_fsm.stop(follower_threshold) == :ok
      assert_received({:EXIT, ^follower_threshold, :normal})
      assert RaftedValue.run_command(leader, :get, 100) == {:error, :timeout}

      # leader should step down if it cannot reach quorum for a while
      {:leader, _} = :sys.get_state(leader)
      :timer.sleep(@t_max_election_timeout)
      {:follower, _} = :sys.get_state(leader)
    end)
  end

  test "3,4,5,6,7 member cluster should elect new leader after leader failure" do
    [3, 4, 5, 6, 7] |> Enum.each(fn n_members ->
      {leader, followers} = make_cluster(n_members - 1)
      assert :gen_fsm.stop(leader) == :ok
      new_leader = wait_until_someone_elected_leader(followers)
      assert RaftedValue.run_command(new_leader, :get) == {:ok, 0}
    end)
  end

  test "other callbacks just do irrelevant things" do
    {leader, followers} = make_cluster(2)

    send(leader, :info_message)
    assert :gen_fsm.send_all_state_event(leader, :foo) == :ok
    result = RaftedValue.status(leader)
    assert_equal_as_set(Map.keys(result), [:from, :members, :leader, :unresponsive_followers, :current_term, :state_name, :config])
    %{members: all, leader: l} = result
    assert Enum.sort(all) == Enum.sort([leader | followers])
    assert l == leader
    assert Process.alive?(leader)

    assert RaftedValue.Server.code_change('old_vsn', :leader, %RaftedValue.Server.State{}, :extra) == {:ok, :leader, %RaftedValue.Server.State{}}
  end

  #
  # property-based tests
  #
  defp client_process_loop(members, value, pending_cmd_tuple \\ nil) do
    receive do
      {:members, new_members} -> client_process_loop(new_members, value, pending_cmd_tuple)
      :finish                 -> :ok
    after
      5 ->
        {cmd, ref, tries} =
          case pending_cmd_tuple do
            nil                  -> {pick_command, make_ref, 20}
            {_cmd, _ref, 0}      -> raise "No leader found, something is wrong!"
            {_cmd, _ref, _tries} -> pending_cmd_tuple
          end
        case run_command(members, cmd, ref) do
          {:ok, ret} ->
            {expected_ret, new_value} = I.command(value, cmd)
            assert ret == expected_ret
            client_process_loop(members, new_value)
          :error ->
            :timer.sleep(200)
            client_process_loop(members, value, {cmd, ref, tries - 1})
        end
    end
  end

  defp pick_command do
    Enum.random([
      :get,
      {:set, 1},
      :inc,
    ])
  end

  defp run_command([], _cmd, _ref), do: :error
  defp run_command([member | members], cmd, ref) do
    case RaftedValue.run_command(member, cmd, 500, ref) do
      {:ok, ret} -> {:ok, ret}
      {:error, {:not_leader, leader}} when is_pid(leader) ->
        members_to_ask = [leader | Enum.reject(members, &(&1 == leader))]
        run_command(members_to_ask, cmd, ref)
      {:error, _} ->
        run_command(members, cmd, ref)
    end
  end

  defp assert_invariances(%{working: working, isolated: isolated} = context) do
    members_alive = working ++ isolated
    member_state_pairs = Enum.map(members_alive, fn m -> {m, :sys.get_state(m)} end)
    new_context =
      Enum.reduce(member_state_pairs, context, fn({member, {state_name, state}}, context0) ->
        {:ok, _} = RaftedValue.Server.State.validate(state)
        assert_server_state_invariance(member, state_name, state)
        assert_server_logs_invariance(state.logs, state.config)
        assert_server_data_invariance(context0, state)
      end)
    assert_cluster_wide_invariance(new_context, member_state_pairs)
  end

  defp assert_server_state_invariance(pid, :leader, state) do
    assert state.members.leader == pid
    assert is_nil(state.members.uncommitted_membership_change) or is_nil(state.members.pending_leader_change)
    assert state.election.voted_for == pid
    refute state.election.timer
    assert state.leadership.heartbeat_timer
    assert state.leadership.quorum_timer

    expected =
      case state.members.uncommitted_membership_change do
        {_, _, :remove_follower, being_removed} -> PidSet.put(state.members.all, being_removed)
        _                                       -> state.members.all
      end
      |> PidSet.delete(pid)
      |> PidSet.to_list
    followers_in_logs = Map.keys(state.logs.followers)
    difference = (followers_in_logs -- expected) ++ (expected -- followers_in_logs)
    assert length(difference) <= 1 # tolerate up to 1 difference
  end

  defp assert_server_state_invariance(pid, :candidate, state) do
    assert state.members.leader == nil
    assert state.election.voted_for == pid
    assert state.election.timer
    refute state.leadership
  end

  defp assert_server_state_invariance(_pid, :follower, state) do
    assert state.election.timer
    refute state.leadership
  end

  defp assert_server_logs_invariance(logs, config) do
    assert logs.i_min       <= logs.i_committed
    assert logs.i_committed <= logs.i_max
    assert_equal_as_set(Map.keys(logs.map), logs.i_min .. logs.i_max)
    assert logs.i_committed - logs.i_min + 1 <= config.max_retained_committed_logs
  end

  defp assert_server_data_invariance(context, state) do
    {_q, map} = state.command_results
    assert map_size(map) <= state.config.max_retained_command_results
    assert_equal_or_put_in_context(context, [:data, state.logs.i_committed], state.data)
  end

  defp assert_cluster_wide_invariance(context0, member_state_pairs) do
    context1 =
      Enum.reduce(member_state_pairs, context0, fn({member, {_, state}}, context) ->
        context
        |> assert_gte_and_put_in_context([:term_numbers  , member], state.current_term)
        |> assert_gte_and_put_in_context([:commit_indices, member], state.logs.i_committed)
      end)
    Enum.filter(member_state_pairs, &match?({_, {:leader, _}}, &1))
    |> Enum.reduce(context1, fn({member, {_, leader_state}}, context) ->
      assert_equal_or_put_in_context(context, [:leaders, leader_state.current_term], member)
    end)
  end

  defp assert_equal_or_put_in_context(context, keys, value) do
    case get_in(context, keys) do
      nil -> put_in(context, keys, value)
      v   ->
        assert value == v
        context
    end
  end

  defp assert_gte_and_put_in_context(context, keys, value) do
    prev_value = get_in(context, keys)
    if prev_value != nil do
      assert prev_value <= value
    end
    put_in(context, keys, value)
  end

  defp pick_operation(%{working: working, killed: killed, isolated: isolated}) do
    n_working  = length(working)
    n_killed   = length(killed)
    n_isolated = length(isolated)
    n_all      = n_working + n_killed + n_isolated
    [
      :op_replace_leader,
      (if n_all < 7   , do: :op_add_follower       ),
      (if n_all > 3   , do: :op_remove_follower    ),
      (if n_all > 3   , do: :op_kill_member        ),
      (if n_killed > 0, do: :op_purge_killed_member),
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.random
  end

  def op_replace_leader(%{working: working, current_leader: leader} = context) do
    followers_in_majority = List.delete(working, leader)
    next_leader = Enum.random(followers_in_majority)
    assert RaftedValue.replace_leader(leader, next_leader) == :ok
    new_leader = receive_leader_elected_message || raise "leader should be elected"
    %{context | current_leader: new_leader}
  end

  def op_add_follower(context) do
    leader = context.current_leader
    new_follower = add_follower(leader)
    assert_receive({:follower_added, pid}, @t_max_election_timeout)
    %{context | working: [new_follower | context.working]}
  end

  def op_remove_follower(%{working: working, killed: killed, isolated: isolated, current_leader: leader} = context) do
    followers_in_majority = List.delete(working, leader)
    all_members = working ++ killed ++ isolated
    target =
      if 2 * length(followers_in_majority) > length(all_members) do # can tolerate 1 member loss
        Enum.random(followers_in_majority)
      else
        nil
      end
    if target do
      assert RaftedValue.remove_follower(leader, target) == :ok
      assert_receive({:follower_removed, ^target}, @t_max_election_timeout)
      assert_receive({:EXIT, ^target, :normal})
      %{context | working: List.delete(working, target)}
    else
      context
    end
  end

  def op_kill_member(%{working: working, killed: killed, isolated: isolated, current_leader: leader} = context) do
    all_members = working ++ killed ++ isolated
    target =
      if 2 * (length(working) - 1) > length(all_members) do
        Enum.random(working)
      else
        if Enum.empty?(isolated), do: nil, else: Enum.random(isolated)
      end
    if target do
      :gen_fsm.stop(target)
      assert_receive({:EXIT, ^target, :normal})
      new_context = %{context | working: List.delete(working, target), killed: [target | killed], isolated: List.delete(isolated, target)}
      if target == leader do
        %{new_context | current_leader: receive_leader_elected_message}
      else
        new_context
      end
    else
      context
    end
  end

  def op_purge_killed_member(%{current_leader: leader, killed: killed} = context) do
    target = Enum.random(killed)
    assert RaftedValue.remove_follower(leader, target) == :ok
    assert_receive({:follower_removed, ^target}, @t_max_election_timeout)
    %{context | killed: List.delete(killed, target)}
  end

  defmodule MessageSendingHook do
    @behaviour RaftedValue.LeaderHook
    def on_command_committed(_, _, _, _), do: nil
    def on_follower_added(pid)          , do: send(:test_runner, {:follower_added, pid})
    def on_follower_removed(pid)        , do: send(:test_runner, {:follower_removed, pid})
    def on_elected                      , do: send(:test_runner, {:elected, self})
  end

  defp receive_leader_elected_message do
    receive do
      {:elected, pid} -> pid
    after
      @t_max_election_timeout * 2 -> nil
    end
  end

  defp finish_client_process(client_pid) do
    :timer.sleep(100)
    refute_received({:EXIT, ^client_pid, _})
    send(client_pid, :finish)
    assert_receive({:EXIT, ^client_pid, :normal}, 1000)
  end

  defp assert_all_members_up_to_date(context) do
    :timer.sleep(250)
    indices = Enum.map(context.working, fn m ->
      {_, state} = :sys.get_state(m)
      assert state.logs.i_committed == state.logs.i_max
      state.logs.i_max
    end)
    assert length(Enum.uniq(indices)) == 1
  end

  test "3,4,5,6,7-member cluster should maintain invariance and keep responsive in the face of minority failure" do
    Process.register(self, :test_runner)
    config = Map.put(@conf, :leader_hook_module, MessageSendingHook)
    {leader, [follower1, follower2]} = make_cluster(2, config)
    assert_received({:follower_added, ^follower1})
    assert_received({:follower_added, ^follower2})

    members = [leader, follower1, follower2]
    context =
      %{working: members, killed: [], isolated: [], current_leader: leader, leaders: %{}, term_numbers: %{}, commit_indices: %{}, data: %{}}
      |> assert_invariances
    client_pid = spawn_link(fn -> client_process_loop(members, I.new) end)

    new_context =
      Enum.reduce(1 .. 50, context, fn(_, c1) ->
        op = pick_operation(c1)
        c2 = apply(__MODULE__, op, [c1]) |> assert_invariances
        send(client_pid, {:members, c2.working})
        c2
      end)

    finish_client_process(client_pid)
    assert_all_members_up_to_date(new_context)
  end

  defmodule CommunicationWithNetsplit do
    def start do
      Agent.start_link(fn -> [] end, name: __MODULE__)
    end

    def set(pids) do
      Agent.update(__MODULE__, fn _ -> pids end)
    end

    defp reachable?(to) do
      isolated = Agent.get(__MODULE__, fn l -> l end)
      !(self in isolated) and !(to in isolated)
    end

    def send_event(server, event) do
      if reachable?(server) do
        :gen_fsm.send_event(server, event)
      else
        :ok
      end
    end

    def reply({to, _} = from, reply) do
      if reachable?(to) do
        :gen_fsm.reply(from, reply)
      else
        :ok
      end
    end
  end

  test "3,4,5,6,7-member cluster should maintain invariance and keep responsive during non-critical netsplit" do
    Process.register(self, :test_runner)
    CommunicationWithNetsplit.start
    config =
      @conf
      |> Map.put(:leader_hook_module, MessageSendingHook)
      |> Map.put(:communication_module, CommunicationWithNetsplit)
    {leader, [follower1, follower2]} = make_cluster(2, config)
    assert_received({:follower_added, ^follower1})
    assert_received({:follower_added, ^follower2})

    members = [leader, follower1, follower2]
    context =
      %{working: members, killed: [], isolated: [], current_leader: leader, leaders: %{}, term_numbers: %{}, commit_indices: %{}, data: %{}}
      |> assert_invariances
    client_pid = spawn_link(fn -> client_process_loop(members, I.new) end)

    new_context =
      Enum.reduce(1 .. 10, context, fn(_, c1) ->
        assert c1.isolated == []
        assert c1.killed   == []

        # cause netsplit
        n_isolated = :rand.uniform(div(length(c1.working) - 1, 2))
        isolated = Enum.take_random(c1.working, n_isolated)
        working_after_split  = c1.working -- isolated
        send(client_pid, {:members, working_after_split})
        CommunicationWithNetsplit.set(isolated)
        leader_after_netsplit =
          if c1.current_leader in isolated do
            receive_leader_elected_message || receive_leader_elected_message || raise "no leader!!!"
          else
            c1.current_leader
          end
        c2 = %{c1 | working: working_after_split, isolated: isolated, current_leader: leader_after_netsplit}

        c5 =
          Enum.reduce(1 .. 5, c2, fn(_, c3) ->
            op = pick_operation(c3)
            c4 = apply(__MODULE__, op, [c3]) |> assert_invariances
            send(client_pid, {:members, c4.working})
            c4
          end)

        # cleanup: recover from netsplit, find new leader, purge killed
        working_after_heal = c5.working ++ c5.isolated
        send(client_pid, {:members, working_after_heal})
        CommunicationWithNetsplit.set([])
        leader_after_heal = receive_leader_elected_message || c5.current_leader
        c6 = %{c5 | working: working_after_heal, isolated: [], current_leader: leader_after_heal}
        Enum.reduce(c6.killed, c6, fn(_, c) -> op_purge_killed_member(c) end)
      end)

    finish_client_process(client_pid)
    assert_all_members_up_to_date(new_context)
  end
end
