# MapReduce Implementation Summary

The system has one master process and multiple worker processes. Workers repeatedly ask the master for work, execute map or reduce tasks, and report completion.

## Task Model

The master tracks two groups of tasks:

- Map tasks, one for each input file.
- Reduce tasks, one for each reduce partition.

Each task has a type, an ID, a state, a start time, and, for map tasks, an input filename.

Task states are:

- `Idle`: not assigned yet.
- `InProgress`: assigned to a worker.
- `Completed`: finished and reported back to the master.

The master also tracks the current phase:

- `MapPhase`
- `ReducePhase`
- `DonePhase`

## RPC Interface

Workers communicate with the master using two RPCs:

- `AskTask`: a worker asks the master for a task.
- `ReportTask`: a worker reports that a task has finished.

The master can return one of four task types:

- `MapTask`: run the application map function.
- `ReduceTask`: run the application reduce function.
- `WaitTask`: no task is ready yet; the worker should sleep and ask again.
- `ExitTask`: the whole job is complete; the worker should exit.

## Map Execution

For a map task, the worker:

1. Opens the input file assigned by the master.
2. Reads the full file content.
3. Calls the plugin-provided `mapf(filename, content)`.
4. Partitions each emitted `KeyValue` by:

   ```go
   ihash(kv.Key) % NReduce
   ```

5. Writes the intermediate data into files named:

   ```text
   mr-X-Y
   ```

   where `X` is the map task ID and `Y` is the reduce task ID.

Intermediate files are encoded with `encoding/json`, so reduce workers can read them back reliably.

## Reduce Execution

For a reduce task with ID `Y`, the worker reads the `Y` bucket produced by every map task:

```text
mr-0-Y
mr-1-Y
...
mr-(NMap-1)-Y
```

The worker then:

1. Decodes all intermediate `KeyValue` records.
2. Sorts them by key.
3. Groups adjacent records with the same key.
4. Calls `reducef(key, values)` once per distinct key.
5. Writes the final output to:

   ```text
   mr-out-Y
   ```

Each output line uses the required format:

```go
fmt.Fprintf(ofile, "%v %v\n", key, output)
```

## Phase Transitions

The master starts in `MapPhase`.

When every map task is marked `Completed`, the master switches to `ReducePhase`.

When every reduce task is marked `Completed`, the master switches to `DonePhase`.

`mrmaster.go` periodically calls `Done()`. Once the master reaches `DonePhase`, `Done()` returns `true`, and the master process exits.

## Worker Loop and Exit

Each worker runs a loop:

1. Call `AskTask`.
2. Execute a map or reduce task if assigned.
3. Sleep briefly on `WaitTask`.
4. Exit on `ExitTask`.

If the worker cannot contact the master, it assumes the master has exited because the job is done, and the worker exits as well.

## Fault Tolerance

The master records the start time of each `InProgress` task. If a task has been running for more than ten seconds, the master assumes the worker may have failed and reassigns that task to another worker.

To avoid exposing partially written files, workers write output to temporary files first and then atomically rename them to their final names.


