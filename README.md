# RaftedValue : A [Raft](https://raft.github.io/) implementation to replicate a value across cluster of Erlang VMs

- [API Documentation](http://hexdocs.pm/rafted_value/)
- [Hex package information](https://hex.pm/packages/rafted_value)

[![Hex.pm](http://img.shields.io/hexpm/v/rafted_value.svg)](https://hex.pm/packages/rafted_value)
[![Build Status](https://travis-ci.org/skirino/rafted_value.svg)](https://travis-ci.org/skirino/rafted_value)
[![Coverage Status](https://coveralls.io/repos/github/skirino/rafted_value/badge.svg?branch=master)](https://coveralls.io/github/skirino/rafted_value?branch=master)

## Design

- Provides [linearizable](https://en.wikipedia.org/wiki/Linearizability) semantics for operations on a stored value.
- Memory-based, no persisted state; restarted members must catch up with its leader from scratch.
- Supports membership changes:
    - adding/removing a member
    - replacing leader
- Each consensus group member is implemented as a [`:gen_fsm`](http://erlang.org/doc/man/gen_fsm.html) process.

## Notes on backward compatibility

- Users of `<= 0.1.8` should upgrade to `0.1.10` before upgrading to `>= 0.2.0`.
    - RPC protocol of `<= 0.1.8` and that of `>= 0.2.0` are slightly incompatible
    - Version `0.1.10` should be able to interact with both `<= 0.1.8` and `>= 0.2.0`

## Example

Suppose there are 3 erlang nodes running:

```ex
$ iex --sname foo -S mix
iex(foo@skirino-Manjaro)1>

$ iex --sname bar -S mix
iex(bar@skirino-Manjaro)1>

$ iex --sname baz -S mix
iex(baz@skirino-Manjaro)1> Node.connect :"foo@skirino-Manjaro"
iex(baz@skirino-Manjaro)1> Node.connect :"bar@skirino-Manjaro"
```

Load the following module in all nodes.

```ex
defmodule QueueWithLength do
  @behaviour RaftedValue.Data
  def new, do: {:queue.new, 0}
  def command({q, l}, {:enqueue, v}) do
    {l, {:queue.in(v, q), l + 1}}
  end
  def command({q1, l}, :dequeue) do
    {{:value, v}, q2} = :queue.out(q1)
    {v, {q2, l - 1}}
  end
  def query({_, l}, :length), do: l
end
```

Then make a 3-member consensus group by spawning one process per node as follows:

```ex
iex(foo@skirino-Manjaro)2> config = RaftedValue.make_config(QueueWithLength)
iex(foo@skirino-Manjaro)3> {:ok, _} = RaftedValue.start_link({:create_new_consensus_group, config}, :foo)

iex(bar@skirino-Manjaro)3> {:ok, _} = RaftedValue.start_link({:join_existing_consensus_group, [{:foo, :"foo@skirino-Manjaro"}]}, :bar)

iex(baz@skirino-Manjaro)3> {:ok, _} = RaftedValue.start_link({:join_existing_consensus_group, [{:foo, :"foo@skirino-Manjaro"}]}, :baz)
```

Now you can run commands on the replicated value:

```ex
iex(foo@skirino-Manjaro)6> RaftedValue.command(:foo, {:enqueue, "a"})
{:ok, 0}
iex(foo@skirino-Manjaro)7> RaftedValue.command(:foo, {:enqueue, "b"})
{:ok, 1}
iex(foo@skirino-Manjaro)8> RaftedValue.command(:foo, {:enqueue, "c"})
{:ok, 2}
iex(foo@skirino-Manjaro)9> RaftedValue.command(:foo, {:enqueue, "d"})
{:ok, 3}
iex(foo@skirino-Manjaro)10> RaftedValue.command(:foo, {:enqueue, "e"})
{:ok, 4}

iex(bar@skirino-Manjaro)4> RaftedValue.command({:foo, :"foo@skirino-Manjaro"}, :dequeue)
{:ok, "a"}
iex(bar@skirino-Manjaro)5> RaftedValue.command({:foo, :"foo@skirino-Manjaro"}, :dequeue)
{:ok, "b"}
iex(bar@skirino-Manjaro)6> RaftedValue.command({:foo, :"foo@skirino-Manjaro"}, {:enqueue, "f"})
{:ok, 3}
iex(bar@skirino-Manjaro)7> RaftedValue.query({:foo, :"foo@skirino-Manjaro"}, :length)
{:ok, 4}
```

The 3-member consensus group keeps on working if 1 member dies:

```ex
iex(baz@skirino-Manjaro)4> :gen_fsm.stop(:baz)

iex(foo@skirino-Manjaro)11> RaftedValue.command(:foo, :dequeue)
{:ok, 3}
```

## Links

- [Raft official website](https://raft.github.io/)
- [The original paper](http://ramcloud.stanford.edu/raft.pdf)
- [The thesis](https://ramcloud.stanford.edu/~ongaro/thesis.pdf)
- [raft_fleet](https://github.com/skirino/raft_fleet)
