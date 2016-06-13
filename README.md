# RaftedValue : A [Raft](https://raft.github.io/) implementation to replicate a value across cluster of Erlang VMs

## Design

- Each member in consensus group is a [`:gen_fsm`](http://erlang.org/doc/man/gen_fsm.html) process.
- Provides [linearizable](https://en.wikipedia.org/wiki/Linearizability) semantics for operations on a stored value.
- Memory-based, no persisted state.
    - You can think of this as an cluster-wide variant of [`Agent`](http://elixir-lang.org/docs/stable/elixir/Agent.html).
- Membership changes:
    - Supports only changes of one member at a time.
    - Supports replacing master.

## Links

- [Raft official website](https://raft.github.io/)
- [The original paper](http://ramcloud.stanford.edu/raft.pdf)
- [The thesis](https://ramcloud.stanford.edu/~ongaro/thesis.pdf)
