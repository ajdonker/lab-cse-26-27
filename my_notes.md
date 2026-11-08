RDD - resilient distributed dataset, collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. 

Users may also ask Spark to persist an RDD in memory, allowing it to be reused efficiently across parallel operations.

Finally, RDDs automatically recover from node failures.

In the PySpark shell, a special interpreter-aware SparkContext is already created for you, in the variable called sc.

While most Spark operations work on RDDs containing any type of objects, a few special operations are only available on RDDs of key-value pairs. The most common ones are distributed “shuffle” operations, such as grouping or aggregating the elements by a key.

Transformations 

groupByKey([numPartitions])   When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs. 

reduceByKey(func, [numPartitions]) When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V.

aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions]) When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. 

join(otherDataset, [numPartitions]) When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. 

coalesce(numPartitions)  Decrease the number of partitions in the RDD to numPartitions.

repartition(numPartitions)   Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network. 

Actions (terminal)

 collect() Return all the elements of the dataset as an array at the driver program. 

 reduce(func) Aggregate the elements of the dataset using a function func (which takes two arguments and returns one).


Certain operations within Spark trigger an event known as the shuffle. The shuffle is Spark’s mechanism for re-distributing data so that it’s grouped differently across partitions. This typically involves copying data across executors and machines, making the shuffle a complex and costly operation.

To understand what happens during the shuffle, we can consider the example of the reduceByKey operation. The reduceByKey operation generates a new RDD where all values for a single key are combined into a tuple - the key and the result of executing a reduce function against all values associated with that key. The challenge is that not all values for a single key necessarily reside on the same partition, or even the same machine, but they must be co-located to compute the result.

In Spark, data is generally not distributed across partitions to be in the necessary place for a specific operation. During computations, a single task will operate on a single partition - thus, to organize all the data for a single reduceByKey reduce task to execute, Spark needs to perform an all-to-all operation. It must read from all partitions to find all the values for all keys, and then bring together values across partitions to compute the final result for each key - this is called the shuffle.

Although the set of elements in each partition of newly shuffled data will be deterministic, and so is the ordering of partitions themselves, the ordering of these elements is not. If one desires predictably ordered data following shuffle then it’s possible to use:

mapPartitions to sort each partition using, for example, .sorted
repartitionAndSortWithinPartitions to efficiently sort partitions while simultaneously repartitioning
sortBy to make a globally ordered RDD

Operations which can cause a shuffle include repartition operations like repartition and coalesce, ‘ByKey operations (except for counting) like groupByKey and reduceByKey, and join operations like cogroup and join.

The Shuffle is an expensive operation since it involves disk I/O, data serialization, and network I/O. To organize data for the shuffle, Spark generates sets of tasks - map tasks to organize the data, and a set of reduce tasks to aggregate it. This nomenclature comes from MapReduce and does not directly relate to Spark’s map and reduce operations.

One of the most important capabilities in Spark is persisting (or caching) a dataset in memory across operations. When you persist an RDD, each node stores any partitions of it that it computes in memory and reuses them in other actions on that dataset (or datasets derived from it). This allows future actions to be much faster (often by more than 10x). Caching is a key tool for iterative algorithms and fast interactive use.