use Croma

defmodule RaftedValue do
  @moduledoc """
  Public interface functions of `RaftedValue` package.
  """

  alias RaftedValue.{TermNumber, Config, Server, Data, LogIndex, Persistence}

  @type consensus_group_info :: {:create_new_consensus_group, Config.t} | {:join_existing_consensus_group, [GenServer.server]}

  @type option :: {:name, atom}
                | {:persistence_dir, Path.t}
                | {:log_file_expansion_factor, number}
                | {:spawn_opt, [:proc_lib.spawn_option]}

  @default_log_file_expansion_factor 10

  @doc """
  Starts a new member of consensus group.

  The 1st argument specifies the consensus group to belong to:

  - `{:create_new_consensus_group, config}`: Creates a new consensus group using the given `RaftedValue.Config.t`.
    The group's sole member is the newly-spawned process and it immediately becomes leader.
  - `{:join_existing_consensus_group, members}`: Joins an already running consensus group as a follower.

  The 2nd argument is a keyword list of options to specify member-specific configurations.

  - `:name`: An atom for local name registration.
  - `:persistence_dir`: Directory path in which both Raft logs and periodic snapshots are stored.
    If not given, the newly-spawned process will run in in-memory mode; it does not persist its state.
    The specified directory will be created if it does not exist.
    Note that it's caller's responsibility to ensure that the specified directory is not used by other `RaftedValue` process.
    See below for details of restoring state from snapshot and logs.
  - `:log_file_expansion_factor`: A number that adjusts when to make a snapshot file.
    This does not take effect if `:persistence_dir` is not given.
    `RaftedValue` keeps track of the file sizes of "currently growing log file" and "previous snapshot file".
    A new snapshot is created when `log_file_size > previous_snapshot_size * expansion_factor`.
    Small value means frequent snapshotting, which can result in high overhead.
    On the other hand larger expansion factor may lead to longer recovery time.
    Defaults to `#{@default_log_file_expansion_factor}`.
  - `:spawn_opt`: A keyword list of options to be passed to [`:erlang.spawn_opt/4`](http://erlang.org/doc/man/erlang.html#spawn_opt-4).

  ## Restoring state from snapshot and log files

  `RaftedValue` implements state recovery using log and snapshot files.
  The recovery procedure is triggered when:

  - A new process is `start_link`ed with `{:create_new_consensus_group, config}` and `:persistence_dir`.
  - Valid log and snapshot files exist in the given `:persistence_dir`.

  Then the newly-spawned process loads the latest snapshot and log files and forms a new 1-member consensus group
  with the restored Raft state (except for membership information).
  When restoring from snapshot and logs, `config` passed in the 1st argument is neglected in favor of configurations in the snapshot and logs.

  If you don't want to restore from snapshot and log files (i.e. want to start a consensus group from scratch),
  then you must clean up the directory before calling `start_link/2`.
  """
  defun start_link(info :: consensus_group_info, options :: [option] \\ []) :: GenServer.on_start do
    case info do
      {:create_new_consensus_group   , %Config{}    }                             -> :ok
      {:join_existing_consensus_group, known_members} when is_list(known_members) -> :ok
      # raise otherwise
    end
    start_link_impl(info, normalize_options(options))
  end

  defp normalize_options(options) do
    Keyword.put_new(options, :log_file_expansion_factor, @default_log_file_expansion_factor)
  end

  defp start_link_impl(info, options0) do
    {spawn_opts, options1} = Keyword.pop(options0, :spawn_opt, [])
    case Keyword.pop(options1, :name) do
      {nil , options2} -> :gen_statem.start_link(                Server, {info, options2}, [spawn_opt: spawn_opts])
      {name, options2} -> :gen_statem.start_link({:local, name}, Server, {info, options2}, [spawn_opt: spawn_opts])
    end
  end

  @doc """
  Make a new instance of `RaftedValue.Config` struct.

  `data_module` must be an implementation of `RaftedValue.Data` behaviour.
  Available options:

  - `leader_hook_module`: An implementation of `RaftedValue.LeaderHook`. Defaults to `RaftedValue.LeaderHook.NoOp`.
  - `communication_module`: A module to define member-to-member async communication (`cast/2` and `reply/2`).
    Defaults to `RaftedValue.RemoteMessageGateway`.
    This is useful for performance optimizations (and also internal testing).
  - `heartbeat_timeout`: Raft's heartbeat timeout in milliseconds. Defaults to `200`.
  - `election_timeout`: Raft's leader election timeout in milliseconds.
    The acrual timeout value in each member is randomly chosen from `election_timeout .. 2 * election_timeout`.
    Defaults to `1000`.
  - `election_timeout_clock_drift_margin`: A time margin in milliseconds to judge whether leader lease has expired or not.
    When a leader gets responses from majority it gets a lease for `election_timeout - margin`.
    During the lease time the leader can assume that no other members are ever elected leader.
    This enables the leader to skip message round trips during processing read-only query.
    Defaults to the value of `election_timeout` (i.e. no lease time, disabling this clock-based optimization).
  - `max_retained_command_results`: Number of command results to be cached,
    in order not to doubly apply the same command due to client retries.
    Defaults to `100`.
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
      max_retained_command_results:        Keyword.get(opts, :max_retained_command_results       , 100),
    } |> Config.new!()
  end

  @type not_leader :: {:not_leader, nil | pid}
  @type remove_follower_error_reason :: not_leader | :uncommitted_membership_change | :not_member | :pending_leader_change | :cannot_remove_leader | :will_break_quorum
  @type replace_leader_error_reason  :: not_leader | :uncommitted_membership_change | :not_member | :new_leader_unresponsive

  @doc """
  Removes a follower from a consensus group.
  """
  defun remove_follower(leader :: GenServer.server, follower_pid :: g[pid]) :: :ok | {:error, remove_follower_error_reason} do
    call(leader, {:remove_follower, follower_pid})
  end

  @doc """
  Replaces current leader of a consensus group from `current_leader` to `new_leader`.
  """
  defun replace_leader(current_leader :: GenServer.server, new_leader :: nil | pid) :: :ok | {:error, replace_leader_error_reason} do
    (current_leader, new_leader) when new_leader == nil or is_pid(new_leader) ->
      call(current_leader, {:replace_leader, new_leader})
  end

  @doc """
  Tell a member to forget about another member.

  The sole purpose of this function is to recover a consensus group from majority failure.
  This operation is unsafe in the sense that it may not preserve invariants of the Raft algorithm
  (for example some committed logs may be lost); use this function as a last resort.

  Membership change introduced by this function is not propagated to other members.
  It is caller's responsibility
  - to stop `member_to_remove` if it is still alive, and
  - to notify all existing members of the membership change.
  """
  defun force_remove_member(member :: GenServer.server, member_to_remove :: pid) :: :ok do
    (member, member_to_remove) when is_pid(member_to_remove) and member != member_to_remove ->
      call(member, {:force_remove_member, member_to_remove})
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
                id          :: command_identifier \\ make_ref(),
                call_module :: module \\ :gen_statem) :: {:ok, Data.command_ret} | {:error, :noproc | :timeout | not_leader} do
    catch_exit(fn ->
      call(leader, {:command, command_arg, id}, timeout, call_module)
    end)
  end

  @doc """
  Executes a read-only query on the stored value of `leader`.

  If the leader does not respond in a timely manner, the function returns `{:error, :timeout}`, i.e., it internally catches exit.
  Caller process should be prepared to handle delayed reply
  (e.g. by dropping delayed reply by `handle_info(_msg, state)`).
  """
  defun query(leader      :: GenServer.server,
              query_arg   :: Data.query_arg,
              timeout     :: timeout \\ 5000,
              call_module :: module \\ :gen_statem) :: {:ok, Data.query_ret} | {:error, :noproc | :timeout | not_leader} do
    catch_exit(fn ->
      call(leader, {:query, query_arg}, timeout, call_module)
    end)
  end

  @doc """
  Executes a read-only query on the stored value of the specified member.

  Unlike `query/3`, this variant allows non-leader members to reply to the query.
  Return value of this function can be stale; it may not reflect recent updates.
  """
  defun query_non_leader(member    :: GenServer.server,
                         query_arg :: Data.query_arg,
                         timeout   :: timeout \\ 5000) :: {:ok, Data.query_ret} | {:error, :noproc | :timeout} do
    catch_exit(fn ->
      call(member, {:query_non_leader, query_arg}, timeout)
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
    call(leader, {:change_config, new_config})
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
  defun status(server :: GenServer.server, timeout :: timeout \\ 5000) :: status_result do
    call(server, :status, timeout)
  end

  @doc """
  Obtains last log index from the log files stored in `dir`.
  """
  defun read_last_log_index(dir :: Path.t) :: nil | LogIndex.t do
    Persistence.read_last_log_index(dir)
  end

  defp call(server, msg, timeout \\ 5000, call_module \\ :gen_statem) do
    call_module.call(server, msg, {:dirty_timeout, timeout})
  end
end
