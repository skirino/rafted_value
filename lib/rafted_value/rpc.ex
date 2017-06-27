use Croma
alias Croma.TypeGen, as: TG

defmodule RaftedValue.RPC do
  alias RaftedValue.{Config, TermNumber, LogIndex, LogInfo, LogEntry, Members, CommandResults, Monotonic}

  defmodule AppendEntriesRequest do
    use Croma.Struct, fields: [
      leader_pid:       Croma.Pid,
      term:             TermNumber,
      prev_log:         LogInfo,
      entries:          TG.list_of(LogEntry),
      i_leader_commit:  LogIndex,
      leader_timestamp: Monotonic,
    ]
  end

  defmodule AppendEntriesResponse do
    use Croma.Struct, fields: [
      from:             Croma.Pid,
      term:             TermNumber,
      success:          Croma.Boolean,
      i_replicated:     TG.nilable(LogIndex),
      leader_timestamp: Monotonic,
    ]
  end

  defmodule RequestVoteRequest do
    use Croma.Struct, fields: [
      term:             TermNumber,
      candidate_pid:    Croma.Atom,
      last_log:         LogInfo,
      replacing_leader: Croma.Boolean,
    ]
  end

  defmodule RequestVoteResponse do
    use Croma.Struct, fields: [
      from:         Croma.Pid,
      term:         TermNumber,
      vote_granted: Croma.Boolean,
    ]
  end

  defmodule InstallSnapshot do
    use Croma.Struct, fields: [
      members:              Members,
      term:                 TermNumber,
      last_committed_entry: LogEntry,
      config:               Config,
      data:                 Croma.Any,
      command_results:      CommandResults,
    ]
  end

  defmodule InstallSnapshotCompressed do
    use Croma.Struct, fields: [
      bin: Croma.Binary,
    ]
  end

  defmodule TimeoutNow do
    use Croma.Struct, fields: [
      append_entries_req: AppendEntriesRequest,
    ]
  end
end
