defmodule RaftedValueTest do
  use ExUnit.Case

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
    if state.members.uncommitted_membership_change do
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
      state = :sys.get_state(m)
      match?({:leader, _}, state)
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
    _follower1 = add_follower(leader)

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
    {all, l} = :gen_fsm.sync_send_all_state_event(leader, :foo)
    assert Enum.sort(all) == Enum.sort([leader | followers])
    assert l == leader
    assert Process.alive?(leader)

    assert RaftedValue.Server.code_change('old_vsn', :leader, %RaftedValue.Server.State{}, :extra) == {:ok, :leader, %RaftedValue.Server.State{}}
  end
end
