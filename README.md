# Spark study

## Architecture

### Low-level APIs
* RDDs
* Distributed variables

### High-level APIs
* DataFrames
* Datasets
* Spark SQL

### Applications
* Streaming
* ML
* GraphX
* Other Libraries

## Spark Cluster Manager
* Driver node + worker nodes
* Standalone, YARN, Mesos

## Spark Processes
* Driver
* Executors

## Execution Mode

### Cluster Mode
* The Spark driver is launched on a worker node
* The cluster manager is responsible for Spark processes

### Client Mode
* The Spark driver is on the client machine
* The client is responsible for the Spark processes and state management

### Local Mode
* The entire process runs locally

### Execution Terminology
A `job` has multiple `stages`, a `stage` has multiple `tasks`.

* `stage` = a set of computations between shuffles.
* `task` = a unit of computation, per partition.
* `DAG` = "directed acyclic graph" of RDD dependencies

* `shuffle` = exchange of data between spark nodes.