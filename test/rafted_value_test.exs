defmodule RaftedValueTest do
  use ExUnit.Case
  alias RaftedValue.PidSet

  @tmp_dir "tmp"

  setup do
    File.rm_rf!(@tmp_dir)
    Process.flag(:trap_exit, true)
    on_exit(fn ->
      File.rm_rf!(@tmp_dir)
    end)
  end

  @conf RaftedValue.make_config(JustAnInt, [
    heartbeat_timeout:                   100,
    election_timeout:                    500,
    election_timeout_clock_drift_margin: 100,
    max_retained_command_results:        10,
  ])

  @t_max_election_timeout @conf.election_timeout * 2

  defp assert_equal_as_set(set1, set2) do
    assert Enum.sort(set1) == Enum.sort(set2)
  end

  defp wait_until_member_change_completes(leader) do
    :timer.sleep(20)
    {:leader, state} = :sys.get_state(leader)
    members = state.members
    if members.uncommitted_membership_change || members.pending_leader_change do
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

  defp add_follower(leader, name \\ nil, dir \\ nil) do
    {:ok, follower} = RaftedValue.start_link({:join_existing_consensus_group, [leader]}, [name: name, persistence_dir: dir])
    wait_until_member_change_completes(leader)
    follower
  end

  defp make_cluster(n_follower, config \\ @conf, persist? \\ false) do
    dir = if persist?, do: Path.join(@tmp_dir, "leader"), else: nil
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, config}, [persistence_dir: dir])
    followers =
      Enum.map(1 .. n_follower, fn i ->
        dir = if persist?, do: Path.join(@tmp_dir, "follower#{i}"), else: nil
        add_follower(leader, nil, dir)
      end)
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
    ref = make_ref()
    send(dest, {:"$gen_sync_event", {self(), ref}, event})
    ref
  end

  test "should appropriately start/add/remove/stop server" do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf}, [name: :foo])
    assert Process.whereis(:foo) == leader
    follower1 = add_follower(leader, :bar)
    assert Process.whereis(:bar) == follower1
    {:ok, follower2} = RaftedValue.start_link({:join_existing_consensus_group, [follower1, leader]}, [name: :baz])
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

    ref1 = simulate_send_sync_event(leader, {:add_follower, self()})
    ref2 = simulate_send_sync_event(leader, {:add_follower, self()})
    assert_receive({^ref1, {:ok, %RaftedValue.RPC.InstallSnapshot{}}})
    assert_receive({^ref2, {:error, :uncommitted_membership_change}})

    wait_until_member_change_completes(leader)

    ref1 = simulate_send_sync_event(leader, {:remove_follower, self()})
    ref2 = simulate_send_sync_event(leader, {:remove_follower, self()})
    assert_receive({^ref1, :ok})
    assert_receive({^ref2, {:error, :uncommitted_membership_change}})

    wait_until_member_change_completes(leader)
  end

  test "should report error when trying to add already joined member" do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf})
    follower1 = add_follower(leader)
    assert :gen_fsm.sync_send_event(leader, {:add_follower, follower1}) == {:error, :already_joined}
  end

  test "should report error when trying to remove leader" do
    {leader, [_follower1, _follower2]} = make_cluster(2)
    assert RaftedValue.remove_follower(leader, leader) == {:error, :cannot_remove_leader}
  end

  test "should report error when trying to remove non member" do
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, @conf})
    assert RaftedValue.remove_follower(leader, self()) == {:error, :not_member}
  end

  test "start_link_and_join_consensus_group should return error and the process should die when no leader found" do
    {:error, _} = RaftedValue.start_link({:join_existing_consensus_group, [:unknown, :member]})
    assert_receive({:EXIT, _pid, _})
  end

  test "should refuse to remove healthy follower if it breaks the current quorum" do
    {leader, [follower1, follower2]} = make_cluster(2)
    assert :gen_fsm.stop(follower1) == :ok
    assert_received({:EXIT, ^follower1, :normal})
    :timer.sleep(@t_max_election_timeout)
    assert RaftedValue.remove_follower(leader, follower2) == {:error, :will_break_quorum}
  end

  test "should remove target follower from unresponsive_follower list" do
    {leader, [follower1]} = make_cluster(1)
    assert RaftedValue.remove_follower(leader, follower1) == :ok
    assert RaftedValue.status(leader)[:unresponsive_followers] == []
    :timer.sleep(@t_max_election_timeout)
    assert RaftedValue.status(leader)[:unresponsive_followers] == [] # removed follower should not be listed as unresponsive
  end

  test "replace_leader should eventually replace leader" do
    {leader, [follower1, follower2]} = make_cluster(2)

    assert RaftedValue.replace_leader(leader, leader) == {:error, :already_leader}

    assert RaftedValue.replace_leader(leader, follower1) == :ok
    wait_until_state_name_changes(leader, :follower)
    wait_until_state_name_changes(follower1, :leader)
    assert RaftedValue.command(leader   , :inc) == {:error, {:not_leader, follower1}}
    assert RaftedValue.command(follower1, :inc) == {:ok, 0}

    assert RaftedValue.replace_leader(follower1, follower2) == :ok
    wait_until_state_name_changes(follower1, :follower)
    wait_until_state_name_changes(follower2, :leader)
    assert RaftedValue.command(follower1, :inc) == {:error, {:not_leader, follower2}}
    assert RaftedValue.command(follower2, :inc) == {:ok, 1}
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
    assert_received({:EXIT, ^follower1, :normal})
    :timer.sleep(@t_max_election_timeout)
    assert RaftedValue.replace_leader(leader, follower1) == {:error, :new_leader_unresponsive}
  end

  test "leader should respond to client requests" do
    {leader, [follower1, follower2]} = make_cluster(2)

    assert RaftedValue.command(leader, :inc     ) == {:ok, 0}
    assert RaftedValue.command(leader, :inc     ) == {:ok, 1}
    assert RaftedValue.command(leader, :inc     ) == {:ok, 2}
    assert RaftedValue.command(leader, :inc     ) == {:ok, 3}
    assert RaftedValue.command(leader, :get     ) == {:ok, 4}
    assert RaftedValue.command(leader, {:set, 1}) == {:ok, 4}
    assert RaftedValue.command(leader, :get     ) == {:ok, 1}
    assert RaftedValue.command(leader, :inc     ) == {:ok, 1}
    assert RaftedValue.command(leader, :inc     ) == {:ok, 2}
    assert RaftedValue.command(leader, :get     ) == {:ok, 3}
    assert RaftedValue.query(  leader, :get     ) == {:ok, 3}

    assert RaftedValue.command(follower1, :get) == {:error, {:not_leader, leader}}
    assert RaftedValue.query(  follower1, :get) == {:error, {:not_leader, leader}}
    assert RaftedValue.command(follower2, :get) == {:error, {:not_leader, leader}}
    assert RaftedValue.query(  follower2, :get) == {:error, {:not_leader, leader}}
  end

  test "read-only query should be handled in the same way as command when election_timeout_clock_drift_margin is large" do
    config = Map.put(@conf, :election_timeout_clock_drift_margin, @conf.election_timeout)
    {leader, [follower1, follower2]} = make_cluster(2, config)

    assert RaftedValue.command(leader , :inc) == {:ok, 0}
    assert RaftedValue.command(leader , :get) == {:ok, 1}
    assert RaftedValue.query(  leader , :get) == {:ok, 1}
    :timer.sleep(100) # for coverage: wait until :query log entry is applied in follwers
    assert RaftedValue.query(  leader , :get) == {:ok, 1}
    assert RaftedValue.query(follower1, :get) == {:error, {:not_leader, leader}}
    assert RaftedValue.query(follower2, :get) == {:error, {:not_leader, leader}}
  end

  test "should not execute the same client requests with identical reference multiple times" do
    {leader, _} = make_cluster(2)

    assert RaftedValue.command(leader, :get     ) == {:ok, 0}
    assert RaftedValue.command(leader, {:set, 1}) == {:ok, 0}
    assert RaftedValue.command(leader, {:set, 2}) == {:ok, 1}
    assert RaftedValue.command(leader, :get     ) == {:ok, 2}

    ref = make_ref()
    assert RaftedValue.command(leader, :inc, 5000, ref) == {:ok, 2}
    assert RaftedValue.command(leader, :inc, 5000, ref) == {:ok, 2}
    assert RaftedValue.command(leader, :inc, 5000, ref) == {:ok, 2}

    assert RaftedValue.command(leader, :get) == {:ok, 3}
  end

  test "1-member cluster should immediately respond to client requests" do
    {leader, _} = make_cluster(0)
    assert RaftedValue.command(leader, :inc) == {:ok, 0}
    assert RaftedValue.query(  leader, :get) == {:ok, 1}
    :timer.sleep(@conf.election_timeout) # should not step down after election timeout
    assert RaftedValue.command(leader, :inc) == {:ok, 1}
    assert RaftedValue.query(  leader, :get) == {:ok, 2}
    :timer.sleep(@conf.election_timeout)
    assert RaftedValue.command(leader, :inc) == {:ok, 2}
    assert RaftedValue.query(  leader, :get) == {:ok, 3}
  end

  test "3,4,5,6,7 member cluster should tolerate up to 1,1,2,2,3 follower failure" do
    [3, 4, 5, 6, 7] |> Enum.each(fn n_members ->
      {leader, followers} = make_cluster(n_members - 1)
      assert RaftedValue.command(leader, {:set, 1}) == {:ok, 0}
      assert RaftedValue.command(leader, :get     ) == {:ok, 1}
      assert RaftedValue.query(  leader, :get     ) == {:ok, 1}

      followers_failing = Enum.take_random(followers, div(n_members - 1, 2))
      Enum.each(followers_failing, fn f ->
        assert :gen_fsm.stop(f) == :ok
        assert_received({:EXIT, ^f, :normal})
        assert RaftedValue.command(leader, :get) == {:ok, 1}
        assert RaftedValue.query(  leader, :get) == {:ok, 1}
      end)

      # leader should not step down as long as it can access majority of members
      {:leader, _} = :sys.get_state(leader)
      :timer.sleep(@t_max_election_timeout)
      {:leader, _} = :sys.get_state(leader)

      follower_threshold = Enum.random(followers -- followers_failing)
      assert :gen_fsm.stop(follower_threshold) == :ok
      assert_received({:EXIT, ^follower_threshold, :normal})
      assert RaftedValue.command(leader, :get, 50) == {:error, :timeout}
      # read-only query succeeds if it is processed within leader's lease
      assert RaftedValue.query(  leader, :get, 50) == {:ok, 1}

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
      assert RaftedValue.command(new_leader, :get) == {:ok, 0}
      assert RaftedValue.query(  new_leader, :get) == {:ok, 0}
    end)
  end

  test "should reject vote request from disruptive ex-member as long as leader is working fine" do
    defmodule DropRemoveFollowerCompleted do
      def send_event(_server, :remove_follower_completed), do: :ok
      def send_event(server, event), do: :gen_fsm.send_event(server, event)
      def reply(from, reply), do: :gen_fsm.reply(from, reply)
    end

    config = Map.put(@conf, :communication_module, DropRemoveFollowerCompleted)
    {leader, [follower1, follower2]} = make_cluster(2, config)

    get_term = fn(pid) ->
      {_, state} = :sys.get_state(pid)
      state.current_term
    end
    leader_term = get_term.(leader)
    assert get_term.(follower2) == leader_term

    assert RaftedValue.remove_follower(leader, follower1) == :ok
    :timer.sleep(@t_max_election_timeout)
    assert Process.alive?(follower1) # :remove_follower_completed message is somehow dropped and `follower1` is still alive
    {:leader, _} = :sys.get_state(leader)
    assert get_term.(leader) == leader_term
    assert get_term.(follower2) == leader_term
  end

  test "force_remove_member should recover a failed consensus group" do
    [3, 5, 7] |> Enum.each(fn n_members ->
      {leader, followers} = make_cluster(n_members - 1)
      remaining_members = Enum.take_random([leader | followers], div(n_members, 2))
      failed_members    = [leader | followers] -- remaining_members

      Enum.each(failed_members, &:gen_fsm.stop/1)
      :timer.sleep(@t_max_election_timeout * 2)
      refute Enum.any?(remaining_members, fn member ->
        RaftedValue.status(member).state_name == :leader
      end)

      for m1 <- remaining_members, m2 <- failed_members do
        assert RaftedValue.force_remove_member(m1, m2) == :ok
      end
      :timer.sleep(@t_max_election_timeout)
      assert Enum.any?(remaining_members, fn member ->
        RaftedValue.status(member).state_name == :leader
      end)
    end)
  end

  test "force_remove_member should return :leader_exists when using it against a healthy consensus group" do
    {leader, followers} = make_cluster(2)
    members1 = [leader | followers]
    members2 = followers ++ [leader]
    Enum.zip(members1, members2) |> Enum.each(fn {member1, member2} ->
      assert RaftedValue.force_remove_member(member1, member2) == {:error, :leader_exists}
    end)
  end

  test "change_config should replace current config field on commit" do
    {leader, followers} = make_cluster(2)
    members = [leader | followers]
    Enum.each(members, fn member ->
      assert RaftedValue.status(member).config == @conf
    end)

    new_conf = Map.update!(@conf, :election_timeout, fn t -> t + 1 end)
    assert RaftedValue.change_config(leader, new_conf) == :ok
    :timer.sleep(@conf.heartbeat_timeout * 2)
    Enum.each(members, fn member ->
      assert RaftedValue.status(member).config == new_conf
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

  defmodule CommunicationWithDelay do
    def send_event(server, event) do
      case event do
        %s{} when s in [RaftedValue.RPC.AppendEntriesRequest, RaftedValue.RPC.AppendEntriesResponse] ->
          spawn(fn ->
            :timer.sleep(40)
            :gen_fsm.send_event(server, event)
          end)
        _ -> :gen_fsm.send_event(server, event)
      end
    end

    def reply(from, reply) do
      :gen_fsm.reply(from, reply)
    end
  end

  test "leader should directly answer queries if leader lease is valid; should not if expired" do
    # carefully crafted configuration to reproduce a bug in rafted_value <= 0.1.8
    config =
      @conf
      |> Map.put(:communication_module, CommunicationWithDelay)
      |> Map.put(:election_timeout, 100)
      |> Map.put(:election_timeout_clock_drift_margin, 1)
      |> Map.put(:heartbeat_timeout, 80)
    {:ok, leader} = RaftedValue.start_link({:create_new_consensus_group, config})
    follower1 = add_follower(leader)
    follower2 = add_follower(leader)

    # Lease time should be calculated from the time AppendEntriesRequest messages are broadcasted from leader to follwers.
    # `command` will take ~80ms for round trip of AppendEntriesRequest and AppendEntriesResponse.
    assert RaftedValue.command(leader, :inc)    == {:ok, 0}
    assert :gen_fsm.stop(follower1)             == :ok
    assert :gen_fsm.stop(follower2)             == :ok
    assert RaftedValue.query(leader, :get)      == {:ok, 1}           # lease available, can answer the query solely by the leader
    :timer.sleep(20)                                                  # 100ms elapsed, lease expired
    assert RaftedValue.query(leader, :get, 100) == {:error, :timeout} # cannot confirm whether the leader's value is still the latest
  end

  test "lonely leader should reply query without making log entry" do
    {:ok, l} = RaftedValue.start_link({:create_new_consensus_group, @conf})
    {:leader, state1} = :sys.get_state(l)
    assert RaftedValue.query(l, :get) == {:ok, 0}
    {:leader, state2} = :sys.get_state(l)
    assert state2.logs == state1.logs
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
            nil                  -> {pick_data_manipulation(), make_ref(), 20}
            {_cmd, _ref, 0}      -> raise "No leader found, something is wrong!"
            {_cmd, _ref, _tries} -> pending_cmd_tuple
          end
        case run_client_request(members, cmd, ref) do
          {:ok, ret} ->
            {expected_ret, new_value} =
              case cmd do
                {:command, arg} -> JustAnInt.command(value, arg)
                {:query  , arg} -> {JustAnInt.query(value, arg), value}
              end
            assert ret == expected_ret
            client_process_loop(members, new_value)
          :error ->
            :timer.sleep(200)
            client_process_loop(members, value, {cmd, ref, tries - 1})
        end
    end
  end

  defp pick_data_manipulation() do
    Enum.random([
      {:command, {:set, :rand.uniform(10)}},
      {:command, :inc},
      {:query, :get},
    ])
  end

  defp run_client_request([], _cmd, _ref), do: :error
  defp run_client_request([member | members], cmd, ref) do
    case call_client_request(member, cmd, ref) do
      {:ok, ret} -> {:ok, ret}
      {:error, {:not_leader, leader}} when is_pid(leader) ->
        members_to_ask = [leader | Enum.reject(members, &(&1 == leader))]
        run_client_request(members_to_ask, cmd, ref)
      {:error, _} ->
        run_client_request(members, cmd, ref)
    end
  end

  defp call_client_request(member, {:command, arg}, ref) do
    RaftedValue.command(member, arg, 500, ref)
  end
  defp call_client_request(member, {:query, arg}, _ref) do
    RaftedValue.query(member, arg, 500)
  end

  defp assert_invariants(%{working: working, isolated: isolated} = context) do
    members_alive = working ++ isolated
    member_state_pairs = Enum.map(members_alive, fn m -> {m, :sys.get_state(m)} end)
    new_context =
      Enum.reduce(member_state_pairs, context, fn({member, {state_name, state}}, context0) ->
        {:ok, _} = RaftedValue.Server.State.validate(state)
        assert_server_state_invariants(member, state_name, state)
        assert_server_logs_invariants(state.logs, state.persistence)
        assert_server_persistence_invariants(state.logs, state.persistence)
        assert_server_data_invariants(context0, state)
      end)
    assert_cluster_wide_invariants(new_context, member_state_pairs)
  end

  defp assert_server_state_invariants(pid, :leader, state) do
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

  defp assert_server_state_invariants(pid, :candidate, state) do
    assert state.members.leader == nil
    assert state.election.voted_for == pid
    assert state.election.timer
    refute state.leadership
  end

  defp assert_server_state_invariants(_pid, :follower, state) do
    assert state.election.timer
    refute state.leadership
  end

  defp assert_server_logs_invariants(logs, persistence) do
    assert logs.i_min       <= logs.i_committed
    assert logs.i_committed <= logs.i_max
    assert_equal_as_set(Map.keys(logs.map), logs.i_min .. logs.i_max)
    if is_nil(persistence) do
      assert logs.i_committed - logs.i_min + 1 <= 100
    end
  end

  defp assert_server_persistence_invariants(logs, persistence) do
    if persistence do
      meta = persistence.latest_snapshot_metadata
      if meta do
        assert meta.last_committed_index <= logs.i_committed
        assert logs.i_min <= meta.last_committed_index + 1 # `logs` must contain all non-compacted entries
      end
    end
  end

  defp assert_server_data_invariants(context, state) do
    {_q, map} = state.command_results
    assert map_size(map) <= state.config.max_retained_command_results
    assert_equal_or_put_in_context(context, [:data, state.logs.i_committed], state.data)
  end

  defp assert_cluster_wide_invariants(context0, member_state_pairs) do
    context1 =
      Enum.reduce(member_state_pairs, context0, fn({member, {_, state}}, context) ->
        context
        |> assert_gte_and_put_in_context([:term_numbers  , member], state.current_term)
        |> assert_gte_and_put_in_context([:commit_indices, member], state.logs.i_committed)
      end)
    leader_pairs = Enum.filter(member_state_pairs, &match?({_, {:leader, _}}, &1))
    assert leader_pairs != []
    context2 = Enum.reduce(leader_pairs, context1, fn({member, {_, leader_state}}, context) ->
      assert_equal_or_put_in_context(context, [:leaders, leader_state.current_term], member)
    end)
    {_, {_, latest_leader_state}} = Enum.max_by(leader_pairs, fn {_, {_, state}} -> state.current_term end)
    assert_gte_and_put_in_context(context2, [:leader_commit_index], latest_leader_state.logs.i_committed)
  end

  defp assert_equal_or_put_in_context(context, keys, value) do
    case get_in(context, keys) do
      nil -> put_in(context, keys, value)
      v   ->
        assert v == value, "#{inspect(keys)} in context has unexpected value #{inspect(v)} (expected: #{inspect(value)})"
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
    |> Enum.random()
  end

  def op_replace_leader(%{working: working, current_leader: leader} = context) do
    followers_in_majority = List.delete(working, leader)
    next_leader = Enum.random(followers_in_majority)
    assert RaftedValue.replace_leader(leader, next_leader) == :ok
    assert_receive({:elected, ^next_leader}, @t_max_election_timeout)
    %{context | current_leader: next_leader}
  end

  def op_add_follower(context) do
    leader = context.current_leader
    persistence_dir =
      case context.persistence_base_dir do
        nil -> nil
        dir ->
          random = :crypto.strong_rand_bytes(10) |> Base.encode16()
          Path.join(dir, random)
      end
    new_follower = add_follower(leader, nil, persistence_dir)
    assert_receive({:follower_added, ^new_follower}, @t_max_election_timeout)
    %{context | working: [new_follower | context.working]}
  end

  def op_remove_follower(%{working: working, killed: killed, isolated: isolated, current_leader: leader} = context) do
    followers_in_majority = List.delete(working, leader)
    all_members = working ++ killed ++ isolated
    if 2 * length(followers_in_majority) > length(all_members) do # can tolerate 1 member loss
      target = Enum.random(followers_in_majority)
      assert RaftedValue.remove_follower(leader, target) == :ok
      assert_receive({:follower_removed, ^target}, @t_max_election_timeout)
      assert_receive({:EXIT, ^target, :normal}, @t_max_election_timeout)
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
        %{new_context | current_leader: receive_leader_elected_message()}
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
    def on_query_answered(_, _, _)      , do: nil
    def on_follower_added(_, pid)       , do: send(:test_runner, {:follower_added, pid})
    def on_follower_removed(_, pid)     , do: send(:test_runner, {:follower_removed, pid})
    def on_elected(_)                   , do: send(:test_runner, {:elected, self()})
    def on_restored_from_files(_)       , do: send(:test_runner, {:restored_from_files, self()})
  end

  defp receive_leader_elected_message() do
    receive do
      {:elected, pid} -> pid
    after
      @t_max_election_timeout * 2 -> nil
    end
  end

  defp start_cluster_and_client(config, persist?) do
    Process.register(self(), :test_runner)
    {leader, [follower1, follower2]} = make_cluster(2, config, persist?)
    assert_receive({:follower_added, ^follower1})
    assert_receive({:follower_added, ^follower2})
    persistence_base_dir = if persist?, do: @tmp_dir, else: nil

    initial_members = [leader, follower1, follower2]
    context =
      %{working: initial_members, killed: [], isolated: [], current_leader: leader, leaders: %{}, term_numbers: %{}, commit_indices: %{}, leader_commit_index: 0, data: %{}, persistence_base_dir: persistence_base_dir}
      |> assert_invariants()
    client_pid = spawn_link(fn -> client_process_loop(initial_members, JustAnInt.new) end)
    {context, client_pid}
  end

  defp repeatedly_change_cluster_configuration(context, client_pid, n) do
    Enum.reduce(1 .. n, context, fn(_, c1) ->
      op = pick_operation(c1)
      c2 = apply(__MODULE__, op, [c1]) |> assert_invariants()
      send(client_pid, {:members, c2.working})
      c2
    end)
  end

  defp finish_client_process(client_pid) do
    refute_received({:EXIT, ^client_pid, _})
    send(client_pid, :finish)
    assert_receive({:EXIT, ^client_pid, :normal}, 1000)
  end

  defp assert_all_members_up_to_date(context) do
    :timer.sleep(@conf.heartbeat_timeout * 2)
    indices = Enum.map(context.working, fn m ->
      {_, state} = :sys.get_state(m)
      assert state.logs.i_committed == state.logs.i_max
      state.logs.i_max
    end)
    assert length(Enum.uniq(indices)) == 1
  end

  defp run_consensus_group_and_check_responsiveness_with_minority_failures(persist?) do
    config = Map.put(@conf, :leader_hook_module, MessageSendingHook)
    {context, client_pid} = start_cluster_and_client(config, persist?)

    new_context = repeatedly_change_cluster_configuration(context, client_pid, 50)

    finish_client_process(client_pid)
    assert_all_members_up_to_date(new_context)
  end

  test "3,4,5,6,7-member cluster should maintain invariants and keep responsive in the face of minority failure (non-persisted)" do
    run_consensus_group_and_check_responsiveness_with_minority_failures(false)
  end

  test "3,4,5,6,7-member cluster should maintain invariants and keep responsive in the face of minority failure (persisted)" do
    run_consensus_group_and_check_responsiveness_with_minority_failures(true)
  end

  defmodule CommunicationWithNetsplit do
    def start() do
      Agent.start_link(fn -> [] end, name: __MODULE__)
    end

    def set(pids) do
      Agent.update(__MODULE__, fn _ -> pids end)
    end

    defp reachable?(to) do
      isolated = Agent.get(__MODULE__, fn l -> l end)
      !(self() in isolated) and !(to in isolated)
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

  defp assert_leader_status(leader, members, isolated) do
    :timer.sleep(@conf.heartbeat_timeout)
    s = RaftedValue.status(leader)
    assert s.state_name == :leader
    assert_equal_as_set(s.members, members)
    assert_equal_as_set(s.unresponsive_followers, isolated)
  end

  defp run_consensus_group_and_check_responsiveness_with_non_critical_netsplit(persist?) do
    CommunicationWithNetsplit.start()
    config =
      @conf
      |> Map.put(:leader_hook_module, MessageSendingHook)
      |> Map.put(:communication_module, CommunicationWithNetsplit)
    {context, client_pid} = start_cluster_and_client(config, persist?)

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
            # although Raft election can take arbitrarily long, trying 3 times is reasonably successful here
            receive_leader_elected_message() || receive_leader_elected_message() || receive_leader_elected_message() || raise "no leader elected after netsplit!"
          else
            :timer.sleep(@t_max_election_timeout) # Wait until the leader recognizes isolated members as unhealthy
            c1.current_leader
          end
        c2 = %{c1 | working: working_after_split, isolated: isolated, current_leader: leader_after_netsplit}
        assert_leader_status(leader_after_netsplit, c1.working, isolated)

        c3 = repeatedly_change_cluster_configuration(c2, client_pid, 5)

        # cleanup: recover from netsplit, find new leader, purge killed
        CommunicationWithNetsplit.set([])
        working_after_heal = c3.working ++ c3.isolated
        send(client_pid, {:members, working_after_heal})
        leader_after_heal = receive_leader_elected_message() || receive_leader_elected_message() || c3.current_leader
        c4 = %{c3 | working: working_after_heal, isolated: [], current_leader: leader_after_heal}
        c5 = Enum.reduce(c4.killed, c4, fn(_, c) -> op_purge_killed_member(c) end)
        assert_leader_status(leader_after_heal, c5.working, [])
        c5
      end)

    finish_client_process(client_pid)
    assert_all_members_up_to_date(new_context)
  end

  test "3,4,5,6,7-member cluster should maintain invariants and keep responsive during non-critical netsplit (non-persisted)" do
    run_consensus_group_and_check_responsiveness_with_non_critical_netsplit(false)
  end

  test "3,4,5,6,7-member cluster should maintain invariants and keep responsive during non-critical netsplit (persisted)" do
    run_consensus_group_and_check_responsiveness_with_non_critical_netsplit(true)
  end
end
