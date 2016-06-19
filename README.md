# RaftedValue : A [Raft](https://raft.github.io/) implementation to replicate a value across cluster of Erlang VMs

- [API Documentation](http://hexdocs.pm/rafted_value/)
- [Hex package information](https://hex.pm/packages/rafted_value)

[![Hex.pm](http://img.shields.io/hexpm/v/rafted_value.svg)](https://hex.pm/packages/rafted_value)
[![Build Status](https://travis-ci.org/skirino/rafted_value.svg)](https://travis-ci.org/skirino/rafted_value)
[![Coverage Status](https://coveralls.io/repos/github/skirino/rafted_value/badge.svg?branch=master)](https://coveralls.io/github/skirino/rafted_value?branch=master)

## Design

- Provides [linearizable](https://en.wikipedia.org/wiki/Linearizability) semantics for operations on a stored value.
- Memory-based, no persisted state.
    - You can think of this as a cluster-wide variant of [`Agent`](http://elixir-lang.org/docs/stable/elixir/Agent.html).
- Membership changes:
    - Supports only changes of one member at a time.
    - Supports replacing master.
- Each consensus group member is a [`:gen_fsm`](http://erlang.org/doc/man/gen_fsm.html) process.

## Links

- [Raft official website](https://raft.github.io/)
- [The original paper](http://ramcloud.stanford.edu/raft.pdf)
- [The thesis](https://ramcloud.stanford.edu/~ongaro/thesis.pdf)
