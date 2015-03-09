# spream
Batch streaming extension (and scalaz.stream integration) for Apache Spark.

Stream processing is natural when data is an ordered sequence, such as a timeseries or a sequence of events. Stream processing is also a memory efficient approach to handling large datasets (whether it is a sequence of not).
But in order to process existing large series datasets quickly, it is necessary to partition them by time and process them in parallel. This is non-trivial when order is important, and when processing depends on past (or future) values - such as moving windows.
      
Note that `spark streaming` is intended for real time stream processing via mini-batching. This does not support offline faster-than-real-time processing and it does not currently allow full parallelisation over pre-existing data (i.e. you need to stream from beginning to end in order, even if you already have all the data on the cluster).

**This work is experimental**
