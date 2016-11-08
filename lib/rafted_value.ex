use Croma

defmodule RaftedValue do
  @moduledoc """
  Public interface functions of `RaftedValue` package.
  """

  alias RaftedValue.{TermNumber, Config, Server, Data}

  @type consensus_group_info :: {:create_new_consensus_group, Config.t} | {:join_existing_consensus_group, [GenServer.server]}

  @doc """
  Starts a new member of consensus group.

  The 1st argument specifies the consensus group to belong to:

  - `{:create_new_consensus_group, Config.t}`: Creates a new consensus group using the given `Config.t`.
    The group's only member is the newly-created process and it is spawned as the leader.
  - `{:join_existing_consensus_group, [servers]}`: Joins an already running consensus grouop as a new follower.

  The second argument is (if given) used for name registration.
  """
  defun start_link(info :: consensus_group_info, name_or_nil :: g[atom] \\ nil) :: GenServer.on_start do
    case info do
      {:create_new_consensus_group   , %Config{}    }                             -> :ok
      {:join_existing_consensus_group, known_members} when is_list(known_members) -> :ok
      # raise otherwise
    end
    start_link_impl(info, name_or_nil)
  end

  defp start_link_impl(info, name) do
    if name do
      :gen_fsm.start_link({:local, name}, Server, info, [])
    else
      :gen_fsm.start_link(Server, info, [])
    end
  end

  @doc """
  Make a new instance of `RaftedValue.Config` struct.

  `data_module` must be an implementation of `RaftedValue.Data` behaviour.
  Available options:

  - `leader_hook_module`: An implementation of `RaftedValue.LeaderHook`. Defaults to `RaftedValue.LeaderHook.NoOp`.
  - `communication_module`: A module to define member-to-member async communication (`send_event/2` and `reply/2`).
    Defaults to `RaftedValue.RemoteMessageGateway`.
    This is configurable for internal testing purpose.
  - `heartbeat_timeout`: Raft's heartbeat timeout in milliseconds. Defaults to `200`.
  - `election_timeout`: Raft's leader election timeout in milliseconds.
    The acrual timeout value in each member is randomly chosen from `election_timeout .. 2 * election_timeout`.
    Defaults to `1000`.
  - `election_timeout_clock_drift_margin`: A time margin in milliseconds to judge whether leader lease has expired or not.
    When a leader gets responses from majority it gets a lease for `election_timeout - margin`.
    During the lease time the leader can assume that no other members are ever elected leader.
    This enables the leader to skip message round trips during processing read-only query.
    Defaults to the value of `election_timeout` (i.e. no lease time, disabling this clock-based optimization).
  - `max_retained_committed_logs`: Number of committed log entries to keep in each member. Defaults to `100`.
  - `max_retained_command_results`: Number of command results to be cached,
    in order to prevent doubly applying the same command. Defaults to `100`.
  """
  defun make_config(data_module :: g[atom], opts :: Keyword.t(any) \\ []) :: Config.t do
    election_timeout = Keyword.get(opts, :election_timeout, 1000)
    %Config{
      data_module:                         data_module,
      leader_hook_module:                  Keyword.get(opts, :leader_hook_module                 , RaftedValue.LeaderHook.NoOp),
      communication_module:                Keyword.get(opts, :communication_module               , RaftedValue.RemoteMessageGateway),
      heartbeat_timeout:                   Keyword.get(opts, :heartbeat_timeout                  , 200),
      election_timeout:                    election_timeout,
      election_timeout_clock_drift_margin: Keyword.get(opts, :election_timeout_clock_drift_margin, election_timeout),
      max_retained_committed_logs:         Keyword.get(opts, :max_retained_committed_logs        , 100),
      max_retained_command_results:        Keyword.get(opts, :max_retained_command_results       , 100),
    } |> Config.validate!
  end

  @type not_leader :: {:not_leader, nil | pid}
  @type remove_follower_error_reason :: not_leader | :uncommitted_membership_change | :not_member | :pending_leader_change | :cannot_remove_leader | :will_break_quorum
  @type replace_leader_error_reason  :: not_leader | :uncommitted_membership_change | :not_member | :new_leader_unresponsive

  @doc """
  Removes a follower from a consensus group.
  """
  defun remove_follower(leader :: GenServer.server, follower_pid :: g[pid]) :: :ok | {:error, remove_follower_error_reason} do
    :gen_fsm.sync_send_event(leader, {:remove_follower, follower_pid})
  end

  @doc """
  Replaces current leader of a consensus group from `current_leader` to `new_leader`.
  """
  defun replace_leader(current_leader :: GenServer.server, new_leader :: nil | pid) :: :ok | {:error, replace_leader_error_reason} do
    (current_leader, new_leader) when new_leader == nil or is_pid(new_leader) ->
      :gen_fsm.sync_send_event(current_leader, {:replace_leader, new_leader})
  end

  @doc """
  Tell a member to forget about another member.

  The sole purpose of this function is to recover a consensus group from majority failure.
  This operation is unsafe in the sense that it may not preserve invariants of the Raft algorithm
  (for example some committed logs may be lost); use this function as a last resort.
  When the receiver process thinks that there exists a valid leader, it rejects with `:leader_exists`.

  Membership change introduced by this function is not propagated to other members.
  It is caller's responsibility to notify all existing members of the membership change.
  """
  defun force_remove_member(member :: GenServer.server, member_to_remove :: pid) :: :ok | {:error, :leader_exists} do
    (member, member_to_remove) when is_pid(member_to_remove) and member != member_to_remove ->
      :gen_fsm.sync_send_all_state_event(member, {:force_remove_member, member_to_remove})
  end

  @type command_identifier :: reference | any

  @doc """
  Executes a command on the stored value of `leader`.

  `id` is an identifier of the command and can be used to filter out duplicate requests.

  If the leader does not respond in a timely manner, the function returns `{:error, :timeout}`, i.e., it internally catches exit.
  Caller process should be prepared to handle delayed reply
  (e.g. by dropping delayed reply by `handle_info(_msg, state)`).
  """
  defun command(leader      :: GenServer.server,
                command_arg :: Data.command_arg,
                timeout     :: timeout \\ 5000,
                id          :: command_identifier \\ make_ref) :: {:ok, Data.command_ret} | {:error, :noproc | :timeout | not_leader} do
    catch_exit(fn ->
      :gen_fsm.sync_send_event(leader, {:command, command_arg, id}, timeout)
    end)
  end

  @doc """
  Executes a read-only query on the stored value of `leader`.

  If the leader does not respond in a timely manner, the function returns `{:error, :timeout}`, i.e., it internally catches exit.
  Caller process should be prepared to handle delayed reply
  (e.g. by dropping delayed reply by `handle_info(_msg, state)`).
  """
  defun query(leader    :: GenServer.server,
              query_arg :: RaftedValue.Data.query_arg,
              timeout   :: timeout \\ 5000) :: {:ok, Data.query_ret} | {:error, :noproc | :timeout | not_leader} do
    catch_exit(fn ->
      :gen_fsm.sync_send_event(leader, {:query, query_arg}, timeout)
    end)
  end

  defp catch_exit(f) do
    try do
      f.()
    catch
      :exit, {a, _} when a in [:noproc, :normal] -> {:error, :noproc}
      :exit, {:timeout, _}                       -> {:error, :timeout}
    end
  end

  @doc """
  Replaces the current configuration.

  The new configuration is replicated (as raft log) to all members.
  """
  defun change_config(leader :: GenServer.server, new_config = %Config{}) :: :ok | {:error, not_leader} do
    :gen_fsm.sync_send_event(leader, {:change_config, new_config})
  end

  @type status_result :: %{
    from:                   pid,
    members:                [pid],
    leader:                 nil | pid,
    unresponsive_followers: [pid],
    current_term:           TermNumber.t,
    state_name:             :leader | :candidate | :follower,
    config:                 Config.t,
  }

  @doc """
  Retrieves status of a member in a consensus group.
  """
  defun status(server :: GenServer.server) :: status_result do
    :gen_fsm.sync_send_all_state_event(server, :status)
  end
end
