use Croma

defmodule RaftedValue do
  alias RaftedValue.{TermNumber, Config, Server}

  @type consensus_group_info :: {:create_new_consensus_group, Config.t} | {:join_existing_consensus_group, [GenServer.server]}

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

  defun make_config(command_module :: g[atom], opts :: Keyword.t(any) \\ []) :: Config.t do
    %Config{
      command_module:               command_module,
      leader_hook_module:           Keyword.get(opts, :leader_hook_module          , RaftedValue.LeaderHook.NoOp),
      communication_module:         Keyword.get(opts, :communication_module        , :gen_fsm),
      heartbeat_timeout:            Keyword.get(opts, :heartbeat_timeout           , 200),
      election_timeout:             Keyword.get(opts, :election_timeout            , 1000),
      max_retained_committed_logs:  Keyword.get(opts, :max_retained_committed_logs , 100),
      max_retained_command_results: Keyword.get(opts, :max_retained_command_results, 100),
    } |> Config.validate!
  end

  defun remove_follower(leader :: GenServer.server, follower_pid :: g[pid]) :: :ok | {:error, atom} do
    :gen_fsm.sync_send_event(leader, {:remove_follower, follower_pid})
  end

  defun replace_leader(current_leader :: GenServer.server, new_leader :: nil | pid) :: :ok | {:error, atom} do
    (current_leader, new_leader) when new_leader == nil or is_pid(new_leader) ->
      catch_exit(fn ->
        :gen_fsm.sync_send_event(current_leader, {:replace_leader, new_leader})
      end)
  end

  @type command_identifier :: reference | any

  defun run_command(leader      :: GenServer.server,
                    command_arg :: RaftedValue.Command.arg,
                    timeout     :: timeout \\ 5000,
                    id          :: command_identifier \\ make_ref) :: {:ok, any} | {:error, atom} do
    catch_exit(fn ->
      :gen_fsm.sync_send_event(leader, {:command, command_arg, id}, timeout)
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

  defun status(server :: GenServer.server) :: status_result do
    :gen_fsm.sync_send_all_state_event(server, :status)
  end
end
