# Query graph

Design doc so Sean can get things out of his head.

Physical operators now currently implement a push-based interface, and a
separate struct helps with execution across some number of operators. At a
high-level, this is what we want, and the current interfaces I think are a
decent prototype. However there's a drawback where operators require a lot of
unnecessary synchronization, making state management within operators tedious.

This document attempts to describe interfaces that reduce synchronization across
parallel partitions while also providing clearer "units of execution".

## Comments on the current interfaces

### Physical operators

These interfaces define the core execution of individual steps during query
execution.

```rust
/// Result of a push to a Sink.
///
/// A sink may not be ready to accept input either because it's waiting on
/// something else to complete (e.g. the right side of a join needs to the left
/// side to complete first) or some internal buffer is full.
pub enum PollPush {
    /// Batch was successfully pushed.
    Pushed,

    /// Batch could not be processed right now.
    ///
    /// A waker will be registered for a later wakeup. This same batch should be
    /// pushed at that time.
    Pending(DataBatch),

    /// This sink requires no more input.
    ///
    /// Upon receiving this, the operator chain should immediately call this
    /// sink's finish method.
    Break,
}

/// Result of a pull from a Source.
#[derive(Debug)]
pub enum PollPull {
    /// Successfully received a data batch.
    Batch(DataBatch),

    /// A batch could not be be retrieved right now.
    ///
    /// A waker will be registered for a later wakeup to try to pull the next
    /// batch.
    Pending,

    /// The source has been exhausted for this partition.
    Exhausted,
}

pub trait Sink: Sync + Send + Explainable + Debug {
    /// Number of input partitions this sink can handle.
    fn input_partitions(&self) -> usize;

    fn poll_push(
        &self,
        task_cx: &TaskContext,
        cx: &mut Context,
        input: DataBatch,
        partition: usize,
    ) -> Result<PollPush>;

    fn finish(&self, task_cx: &TaskContext, partition: usize) -> Result<()>;
}

pub trait Source: Sync + Send + Explainable + Debug {
    /// Number of output partitions this source can produce.
    fn output_partitions(&self) -> usize;

    fn poll_pull(
        &self,
        task_cx: &TaskContext,
        cx: &mut Context,
        partition: usize,
    ) -> Result<PollPull>;
}

pub trait PhysicalOperator: Sync + Send + Explainable + Debug {
    /// Execute this operator on an input batch.
    fn execute(&self, task_cx: &TaskContext, input: DataBatch) -> Result<DataBatch>;
}
```

Good:

- `PollPush` and `PollPull`. These are pretty flexible and map nicely to
  `std::task::Poll` (just with a bit more ergnomics around returning a
  `Result`).
- A pretty clear split between stateful (`Sink` + `Source`) operators like
  joins, and stateless (`PhysicalOperator`) like filter. Naming could be better
  though.

Bad:

- Stateful operator implementations require that they keep track of the state
  internal to the operator. This is the source of unecessary synchronization. A
  partition should not have to synchronize if its work can be done independent
  of any other partition.
  
  For example, the build side of the nested loop join:
  
  ```rust
  #[derive(Debug)]
  pub struct PhysicalNestedLoopJoinBuildSink {
      /// Partition-local states.
      states: Arc<Vec<Mutex<LocalState>>>,

      /// Number of partitions we're still waiting on to complete.
      remaining: AtomicUsize,
  }
  ```

  The build side of the nested loop join should not need to synchronize until
  that partitions input is complete. Roughly, the `poll_push` implementation
  should never lock, while the `finish` would lock to coordinate with a global
  state.

Unknown:

- The `TaskContext` that's passed in to the various methods was entirely to
  provide operators with handle for modifying the calling session (e.g. setting
  a session variable). We need that functionality, but the way it's currently
  being done probably isn't it.
  
  I also originally had the idea of using `TaskContext` to store performance
  metrics of the operators, but I no longer think that's a good idea
  (`TaskContext` can't be mutable and so there would need to be more
  synchronization). I plan to address the performance metrics later in this
  document.
  
### Operator chain

An operator chain is essentially a subset of the query.

```rust
/// An operator chain represents a subset of computation in the execution
/// pipeline.
///
/// During execution, batches are pulled from a source, ran through a sequence
/// of stateless operators, then pushed to a sink.
#[derive(Debug)]
pub struct OperatorChain {
    /// Sink where all output batches are pushed to.
    sink: Box<dyn Sink>,

    /// Sequence of operators to run batches through. Executed left to right.
    operators: Vec<Box<dyn PhysicalOperator>>,

    /// Where to pull originating batches.
    source: Box<dyn Source>,

    /// Partition-local states.
    states: Vec<Mutex<PartitionState>>,
}
```

Good:

- I think the general approach is good in terms of having a core unit of
  execution.
  
Bad:

- This handles _all_ partitions for this subset of the query. I think there's a
  possibility to break this down such that we can avoid the mutexes. Ideally
  this requires no mutexes, and a thread is able to operator on this with
  mutable access.

Unknown:

### Pipeline

Essentially the full query represented in multiple operator chains. This is what
the scheduler currently accepts.

```rust
#[derive(Debug)]
pub struct Pipeline {
    /// The operator chains that make up this pipeline.
    pub chains: Vec<Arc<OperatorChain>>,
}
```

Good:

- Simple. There's no fancy routing that has to happen as each operator can be
  called arbitrarily thanks to the `Poll...` stuff. If there's nothing to do,
  that chain will be called agains once it is ready to be executed.
  
  This simplicity actually ends up making the scheduler pretty simple too
  (currently `scheduler.rs` is 141 lines long).

Bad:

Unknown:

## Proposal

Operator logic and operator states should be separate. By having the state
separate from the logic (the actual operator itself), we can reduce
synchronization across partitions, and provide better DX.

### Global/local states

Each (stateful) operator will define two states that will be used during
execution; the "local" state and the "global" state.

The "local" state will be for state that's local to the partition. For example,
the build-side hash join operator would have a partition-local hash table in its
local state. During execution, the operator will receive a mutable reference to
its local state, allowing direct modification with no synchronization.

The "global" state is for state that needs to be shared across all partitions.
For example, the build-side hash join operator would have a global hash table
that's written to from each partition with each partition's local hash table.
During execution, the operator will receive a shared reference to the global
state. Modifying the global state will require internal mutation through
mutexes/atomics/etc.

Since we will only support a fixed number of operators, each operator will have
a variant in each of the global and local state enums:

```rust
pub enum GlobalSinkState {
    PhysicalHashJoin(PhysicalHashJoinGlobalSinkState),
    ...
}

pub enum LocalSinkState {
    PhysicalHashJoin(PhysicalHashJoinLocalSinkState),
    ...
}

pub enum GlobalSourceState {
    PhysicalHashJoin(PhysicalHashJoinGlobalSourceState),
    ...
}

pub enum LocalSourceState {
    PhysicalHashJoin(PhysicalHashJoinLocalSourceState),
    ...
}
```

TODO: Determine if we want/need separate states for sources and sinks.

### Operator interfaces

Slightly modified from our current trait definitions. `PollPush` and `PollPull`
will remain the same.

```rust
pub trait SinkOperator: Sync + Send + Explainable + Debug {
    fn input_partition(&self) -> usize;

    /// Initialize the local state for a partition.
    ///
    /// This should be called once per partition.
    fn init_local_state(&self, partition: usize) -> Result<LocalSinkState>;

    /// Initialize the global state.
    ///
    /// This should be called once in total.
    fn init_global_state(&self) -> Result<GlobalSinkState>;

    /// Try to push a batch for this partition.
    fn poll_push(
        &self,
        cx: &mut Context,
        local: &mut LocalSinkState,
        global: &GlobalSinkState,
        input: Batch,
        partition: usize,
    ) -> Result<PollPush>;

    /// Indicate that we're done pushing to this partition.
    fn finish(
        &self,
        local: &mut LocalSinkState,
        global: &GlobalSinkState,
        partition: usize
    ) -> Result<()>;
}

pub trait SourceOperator: Sync + Send + Explainable + Debug {
    fn output_partitions(&self) -> usize;

    /// Initialize the local state for a partition.
    ///
    /// This should be called once per partition.
    fn init_local_state(&self, partition: usize) -> Result<LocalSourceState>;

    /// Initialize the global state.
    ///
    /// This should be called once in total.
    fn init_global_state(&self) -> Result<GlobalSourceState>;

    /// Try to pull a batch for this partition.
    fn poll_pull(
        &self,
        cx: &mut Context,
        local: &mut LocalSourceState,
        global: &GlobalSourceState,
        partition: usize,
    ) -> Result<PollPull>;
}

/// Operator that requires no state (filter, projection)
pub trait StatelessOperator: Sync + Send + Explainable + Debug {
    fn execute(&self, input: Batch) -> Result<Batch>;
}
```

