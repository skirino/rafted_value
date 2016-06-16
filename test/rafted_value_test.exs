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

  defp wait_until_member_change_completes(leader) do
    :timer.sleep(10)
    {:leader, state} = :sys.get_state(leader)
    members = state.members
    if members.uncommitted_membership_change && state.pending_leader_change do
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

  defp make_cluster(n_follower) do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf})
    followers = Enum.map(1 .. n_follower, fn _ -> add_follower(leader) end)
    {leader, followers}
  end

  defp wait_until_someone_elected_leader(members) do
    :timer.sleep(100)
    leader = Enum.find(members, fn m ->
      match?({:leader, _}, :sys.get_state(m))
    end)
    if leader do
      leader
    else
      wait_until_someone_elected_leader(members)
    end
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

    assert RaftedValue.leave_consensus_group_and_stop(:bar) == :ok
    wait_until_member_change_completes(leader)
    assert_receive({:EXIT, ^follower1, :normal})
    :timer.sleep(100) # for coverage: wait until :remove_follower log entry is applied in follwer2

    assert RaftedValue.leave_consensus_group_and_stop(:baz) == :ok
    wait_until_member_change_completes(leader)
    assert_receive({:EXIT, ^follower2, :normal})

    assert RaftedValue.leave_consensus_group_and_stop(:foo) == :ok
    wait_until_member_change_completes(leader)
    assert Process.alive?(leader)

    assert :gen_fsm.stop(leader) == :ok
    refute Process.alive?(leader)
  end

  test "should not concurrently execute multiple membership changes" do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf})
    follower1 = add_follower(leader)

    simulate_send_sync_event = fn(event) ->
      ref = make_ref
      send(leader, {:"$gen_sync_event", {self, ref}, event})
      ref
    end

    ref1 = simulate_send_sync_event.({:add_follower, self})
    ref2 = simulate_send_sync_event.({:add_follower, self})
    assert_receive({^ref1, {:ok, %RaftedValue.RPC.InstallSnapshot{}}})
    assert_receive({^ref2, {:error, :uncommitted_membership_change}})

    wait_until_member_change_completes(leader)

    ref1 = simulate_send_sync_event.({:remove_follower, self})
    ref2 = simulate_send_sync_event.({:remove_follower, self})
    assert_receive({^ref1, :ok})
    assert_receive({^ref2, {:error, :uncommitted_membership_change}})

    wait_until_member_change_completes(leader)

    ref1 = simulate_send_sync_event.({:replace_leader, follower1})
    ref2 = simulate_send_sync_event.({:replace_leader, follower1})
    assert_receive({^ref1, :ok})
    assert_receive({^ref2, {:error, :pending_leader_change}})

    wait_until_state_name_changes(leader, :follower)
    wait_until_state_name_changes(follower1, :leader)
  end

  test "should report error when trying to add already joined member" do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf})
    follower1 = add_follower(leader)
    assert :gen_fsm.sync_send_event(leader, {:add_follower, follower1}) == {:error, :already_joined}
  end

  test "should report error when trying to remove non member" do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf})
    assert :gen_fsm.sync_send_event(leader, {:remove_follower, self}) == {:error, :not_member}
  end

  test "start_link_and_join_consensus_group should return error and the process should die when no leader found" do
    {:error, _} = RaftedValue.start_link({:join_existing_consensus_group, [:unknown, :member]})
    assert_receive({:EXIT, _pid, _})
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
    :timer.sleep(500) # should not step down after election timeout
    assert RaftedValue.run_command(leader, :inc) == {:ok, 1}
    :timer.sleep(500)
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
      :timer.sleep(@conf.election_timeout * 2)
      {:leader, _} = :sys.get_state(leader)

      follower_threshold = Enum.random(followers -- followers_failing)
      assert :gen_fsm.stop(follower_threshold) == :ok
      assert_received({:EXIT, ^follower_threshold, :normal})
      catch_exit RaftedValue.run_command(leader, :get, 100)

      # leader should step down if it cannot reach quorum for a while
      {:leader, _} = :sys.get_state(leader)
      :timer.sleep(@conf.election_timeout * 2)
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
    %{members: all, leader: l} = RaftedValue.status(leader)
    assert Enum.sort(all) == Enum.sort([leader | followers])
    assert l == leader
    assert Process.alive?(leader)

    assert RaftedValue.Server.code_change('old_vsn', :leader, %RaftedValue.Server.State{}, :extra) == {:ok, :leader, %RaftedValue.Server.State{}}
  end

  #
  # property-based tests
  #
  defp client_process_loop(members, value) do
    receive do
      {:members, current_members} -> client_process_loop(current_members, value)
      :finish                     -> :ok
    after
      0 ->
        cmd = pick_command
        {expected_ret, new_value} = I.command(value, cmd)
        new_members1 = Enum.filter(members, &Process.alive?/1)
        {ret, new_members2} = run_command(new_members1, cmd)
        assert ret == expected_ret
        client_process_loop(new_members2, new_value)
    end
  end

  defp pick_command do
    Enum.random([
      :get,
      {:set, 1},
      :inc,
    ])
  end

  defp run_command(members, cmd) do
    run_command_with_retry(members, cmd, make_ref, 5)
  end

  defp run_command_with_retry(members, cmd, ref, n) do
    if n == 0 do
      raise "No leader found! Something is wrong!"
    else
      case run_command(members, members, cmd, ref) do
        nil ->
          :timer.sleep(300)
          run_command_with_retry(members, cmd, ref, n - 1)
        r -> r
      end
    end
  end

  defp run_command([], _, _, _), do: nil
  defp run_command([member | members], all, cmd, ref) do
    case RaftedValue.run_command(member, cmd, 1000, ref) do
      {:ok, ret} -> {ret, all}
      {:error, {:not_leader, leader}} when is_pid(leader) ->
        new_all        = [leader | Enum.reject(all    , &(&1 == leader))]
        members_to_ask = [leader | Enum.reject(members, &(&1 == leader))]
        run_command(members_to_ask, new_all, cmd, ref)
      {:error, _} ->
        run_command(members, all, cmd, ref)
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
    refute state.leadership.heartbeat_timer
    refute state.leadership.quorum_timer
  end

  defp assert_server_state_invariance(_pid, :follower, state) do
    assert state.election.timer
    refute state.leadership.heartbeat_timer
    refute state.leadership.quorum_timer
  end

  defp assert_equal_set(set1, set2) do
    assert Enum.sort(set1) == Enum.sort(set2)
  end

  defp assert_server_logs_invariance(logs, config) do
    assert logs.i_min       <= logs.i_committed
    assert logs.i_committed <= logs.i_max
    assert_equal_set(Map.keys(logs.map), logs.i_min .. logs.i_max)
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
    wait_until_state_name_changes(next_leader, :leader)
    %{context | current_leader: next_leader}
  end

  def op_add_follower(context) do
    leader = context.current_leader
    new_follower = add_follower(leader)
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
      assert RaftedValue.leave_consensus_group_and_stop(target) == :ok
      :timer.sleep(1000)
      assert_receive({:EXIT, ^target, :normal})
      wait_until_member_change_completes(leader)
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
        :timer.sleep(1000)
        new_leader = find_leader(new_context.working)
        %{new_context | current_leader: new_leader}
      else
        new_context
      end
    else
      context
    end
  end

  def op_purge_killed_member(%{current_leader: leader, killed: killed} = context) do
    target = Enum.random(killed)
    assert :gen_fsm.sync_send_event(leader, {:remove_follower, target}) == :ok
    %{context | killed: List.delete(killed, target)}
  end

  defp find_leader(members, n \\ 2) do
    if n == 0 do
      raise "no leader found!"
    else
      :timer.sleep(100)
      Enum.find_value(members, fn m ->
        %{leader: leader} = RaftedValue.status(m)
        leader
      end) || find_leader(members, n - 1)
    end
  end

  test "3,4,5,6,7-member cluster should maintain invariance and keep responsive in the face of minority failure" do
    {leader, followers} = make_cluster(2)
    members = [leader | followers]
    context = %{working: members, killed: [], isolated: [], current_leader: leader, leaders: %{}, term_numbers: %{}, commit_indices: %{}, data: %{}}
    assert_invariances(context)
    client_pid = spawn_link(fn -> client_process_loop(members, I.new) end)

    new_context =
      Enum.reduce(1 .. 100, context, fn(_, c1) ->
        op = pick_operation(c1)
        c2 = apply(__MODULE__, op, [c1]) |> assert_invariances
        send(client_pid, {:members, c2.working})
        c2
      end)

    :timer.sleep(100)
    refute_received({:EXIT, ^client_pid, _})
    send(client_pid, :finish)
    assert_receive({:EXIT, ^client_pid, :normal}, 1000)
    :timer.sleep(250)

    surviving_members = new_context.working
    indices =
      Enum.map(surviving_members, fn m ->
        {_, state} = :sys.get_state(m)
        assert state.logs.i_committed == state.logs.i_max
        state.logs.i_max
      end)
    assert length(Enum.uniq(indices)) == 1
  end
end
