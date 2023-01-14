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