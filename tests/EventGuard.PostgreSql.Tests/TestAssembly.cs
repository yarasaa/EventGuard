// Both TFMs share the small local PostgreSQL instance; concurrency inside each contract remains explicit.
[assembly: Xunit.CollectionBehavior(MaxParallelThreads = 3)]
