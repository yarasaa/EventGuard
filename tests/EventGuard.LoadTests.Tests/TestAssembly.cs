// Cases share local database catalogs. Keep fixture DDL serial; concurrency inside
// each real WorkPump/admission scenario remains enabled and explicitly bounded.
[assembly: Xunit.CollectionBehavior(DisableTestParallelization = true)]
