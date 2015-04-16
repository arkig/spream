# spream
Batch streaming extension (and [scalaz.stream](https://github.com/scalaz/scalaz-stream) integration) 
for [Apache Spark](https://spark.apache.org/).

Stream processing is natural when data is an ordered sequence, such as a timeseries or a sequence of events. 
Stream processing is also a memory efficient approach to handling large datasets (whether it is a sequence of not). 
But in order to process existing large series datasets quickly, it is necessary to partition them by time and process 
them in parallel. This is non-trivial when order is important, and when processing depends on past (or future) 
values - such as moving windows. This project solves these problems using 
[scalaz.stream](https://github.com/scalaz/scalaz-stream)s and [Apache Spark](https://spark.apache.org/).
 
Key features:

- Partition a very long series (in an ordered RDD) into roughly equally sized partitions with overlap (key distribution is 
estimated so that this still works when keys are not regularly spaced).
- Resulting partitions will have enough past and future data to facilitate moving window stream operations without any gaps.
- Write the output of large `scalaz.stream.Process[Task,O]` or `scalaz.stream.Process[Nothing,O]`s directly into RDD 
partitions (and without resorting to buffered queues, `runLog`, etc). 
- Run `scalaz.stream.Process1[I,O]` over partitions.
- Small library of `scalaz.stream.Process1[I,O]`s that handle partitioning information for you when 
computing over moving windows. 
- Optimisations for selecting sub-series (`import spream.Spream._` and then you can use `rdd.filterByRange2(...)`) 

The result is that you can run distributed moving window computations on big data on a cluster, almost as easily as if
you were doing it on small data on one node. Hence, it's easy to scale out certain types of processing 
already written in `scalaz.stream`. You can also chop up and select sub-sequences of series efficiently.  

The [integration tests](src/test/scala/spream/IntegrationSuite.scala) provide some examples. 

**Please note that this work is experimental**

## Programming Tips

- Note that `scalaz.stream.Process[Task,O]`s are not serializable and they do not need to be serialized for
this to work. If you code it correctly they get instantiated on the worker. If you don't, you get an exception. 
In contrast, note that `Process1`s are serializable, so you can instantiate them on the driver (though I find that 
is a little less readable in most cases).  
- You now have two options to map, flatMap, etc over an RDD - as part of the `Process`or running in the 
partitions or on the RDD itself. Prefer the former because it should(?) prevent an additional pass.
- Try to load data into your RDD so that it is already sorted. For example, take advantage of the fact that
partitions have a fixed order. This will save you a `sortBy`.    
- Preserve the partitioner when you run a `process1 p` that does not change key ordering (the usual case when
stream processing) over a series partitioned RDD, e.g. 
`val rdd2 = rdd.mapPartitions(IteratorConversions.process1ToIterators(p),true)` -- note the `true`. 
This will enable more efficient range queries (i.e. `rdd2.filterByRange2(...)`) on the result. 

## Nice to have...

- A specific RDD subtype for such window partitioned data (better type safety, etc). 
- Better Spark integration - for example, the current `RangePartitioner` in Spark is not written in an extensible way. 
This does not allow the `spream.PartitionedSeriesPartitioner` to be implemented as a subtype. This in turn makes it 
trickier to integrate optimisations. 
  
## Limitations

- Be careful with moving window operations that require last known value (LKV) interpolation at some offset in the past, 
since the LKV can be arbitrarily far in the past (i.e. beyond any window used when partitioning). This case is not 
covered in this library, because a) it means there is a strong dependency on the data, which would break the mechanism 
which allows the window partitioning to be implemented efficiently, and b) use cases where a meaningful past window bound 
cannot be set are rare (and probably bad!). For example, if you need a moving average with decay, it's probably 
reasonable to cut off the long tail of your decay function.  
      
## What about Spark Streaming?
      
[Spark streaming](http://spark.apache.org/docs/latest/streaming-programming-guide.html) is intended for real time 
stream processing via mini-batching. This does not support offline faster-than-real-time processing and it does not 
currently allow full parallelisation over pre-existing data (i.e. you need to stream from beginning to end 
in order, even if you already have all the data on the cluster).



