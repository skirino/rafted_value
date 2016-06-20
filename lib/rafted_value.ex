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
    This is configurable for internal testing purpose. Defaults to `:gen_fsm`.
  - `heartbeat_timeout`: Raft's heartbeat timeout in milliseconds. Defaults to `1000`.
  - `election_timeout`: Raft's leader election timeout in milliseconds.
    The acrual timeout value in each member is randomly chosen from `election_timeout .. 2 * election_timeout`.
    Defaults to `200`.
  - `election_timeout_clock_drift_margin`: A time margin in milliseconds to judge whether leader lease has expired or not.
    When a leader gets responses from majority it gets a lease for `election_timeout - margin`.
    During the lease the leader can assume that no other members have elected leader.
    This enables the leader to skip message round trip during processing read-only query.
    Defaults to the value of `election_timeout` (i.e. no lease time, disabling this clock-based optimization).
  - `max_retained_committed_lgos`: Number of committed log entries to keep in each member. Defaults to `100`.
  - `max_retained_command_results`: Number of command results to be cached,
    in order to prevent doubly applying the same command. Defaults to `100`.
  """
  defun make_config(data_module :: g[atom], opts :: Keyword.t(any) \\ []) :: Config.t do
    election_timeout = Keyword.get(opts, :election_timeout, 1000)
    %Config{
      data_module:                         data_module,
      leader_hook_module:                  Keyword.get(opts, :leader_hook_module                 , RaftedValue.LeaderHook.NoOp),
      communication_module:                Keyword.get(opts, :communication_module               , :gen_fsm),
      heartbeat_timeout:                   Keyword.get(opts, :heartbeat_timeout                  , 200),
      election_timeout:                    election_timeout,
      election_timeout_clock_drift_margin: Keyword.get(opts, :election_timeout_clock_drift_margin, election_timeout),
      max_retained_committed_logs:         Keyword.get(opts, :max_retained_committed_logs        , 100),
      max_retained_command_results:        Keyword.get(opts, :max_retained_command_results       , 100),
    } |> Config.validate!
  end

  @doc """
  Removes a follower from a consensus group.
  """
  defun remove_follower(leader :: GenServer.server, follower_pid :: g[pid]) :: :ok | {:error, atom} do
    :gen_fsm.sync_send_event(leader, {:remove_follower, follower_pid})
  end

  @doc """
  Replaces current leader of a consensus group from `current_leader` to `new_leader`.
  """
  defun replace_leader(current_leader :: GenServer.server, new_leader :: nil | pid) :: :ok | {:error, atom} do
    (current_leader, new_leader) when new_leader == nil or is_pid(new_leader) ->
      catch_exit(fn ->
        :gen_fsm.sync_send_event(current_leader, {:replace_leader, new_leader})
      end)
  end

  @type command_identifier :: reference | any

  @doc """
  Executes a command on the stored value of `leader`.
  """
  defun command(leader      :: GenServer.server,
                command_arg :: Data.command_arg,
                timeout     :: timeout \\ 5000,
                id          :: command_identifier \\ make_ref) :: {:ok, Data.command_ret} | {:error, atom} do
    catch_exit(fn ->
      :gen_fsm.sync_send_event(leader, {:command, command_arg, id}, timeout)
    end)
  end

  @doc """
  Executes a read-only query operation on the stored value of `leader`.
  """
  defun query(leader    :: GenServer.server,
              query_arg :: RaftedValue.Data.query_arg,
              timeout   :: timeout \\ 5000) :: {:ok, Data.query_ret} | {:error, atom} do
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
