namespace EventGuard.Core.Tests;

public sealed class NativeBatchContractsTests
{
    private static ExternalAction Action(ProducerScope scope, Guid? id = null) => new(scope,id ?? Guid.NewGuid(),"service","v1",[1]);
    [Fact]
    public void Intent_pages_are_bounded_scoped_and_immutable()
    {
        var scope=new ProducerScope("tenant","producer");var action=Action(scope);
        Assert.Throws<ArgumentException>(()=>new ExternalActionEnqueueBatch([]));
        Assert.Throws<ArgumentException>(()=>new ExternalActionEnqueueBatch([new(action),new(action)]));
        Assert.Throws<ArgumentException>(()=>new ExternalActionEnqueueBatch(Enumerable.Range(0,1001).Select(_=>new ExternalActionIntent(Action(scope)))));
        Assert.Throws<ArgumentException>(()=>new ExternalActionEnqueueBatch([new(action),new(Action(new("other","producer")))]));
        Assert.Throws<ArgumentException>(()=>new ExternalActionEnqueueBatch([new(action,new(new("other","producer"),"projection","v1"))]));
        var source=new[]{new ExternalActionIntent(action)};var page=new ExternalActionEnqueueBatch(source);
        source[0]=new(Action(scope));Assert.Same(action,page.Items[0].Action);
    }
    [Fact]
    public void Intent_byte_budget_is_checked_before_database_access()
    {
        var scope=new ProducerScope("tenant","producer");var payload=new byte[1024*1024];
        Assert.Throws<ArgumentException>(()=>new ExternalActionEnqueueBatch(Enumerable.Range(0,65)
            .Select(_=>new ExternalActionIntent(new(scope,Guid.NewGuid(),"service","v1",payload)))));
    }
    [Theory]
    [InlineData(0,8388608)]
    [InlineData(1001,8388608)]
    [InlineData(100,1)]
    [InlineData(100,67108865)]
    public void Projection_pages_reject_unbounded_or_starving_limits(int items,int bytes) =>
        Assert.Throws<ArgumentOutOfRangeException>(()=>new ExternalResultBatchOptions(items,bytes));
    [Fact]
    public void Permit_ids_retain_timestamp_and_entropy_and_legacy_ids_are_distinguished()
    {
        var before=DateTimeOffset.UtcNow.AddMilliseconds(-1);
        var ids=Enumerable.Range(0,1000).Select(_=>ExternalCallRequestIds.New()).ToArray();
        var after=DateTimeOffset.UtcNow;
        Assert.Equal(ids.Length,ids.Distinct().Count());
        Assert.All(ids,id=>Assert.InRange(ExternalCallRequestIds.Timestamp(id)!.Value,before,after));
        Assert.Null(ExternalCallRequestIds.Timestamp(Guid.NewGuid()));
        Assert.Throws<ArgumentException>(()=>ExternalCallRequestIds.Timestamp(Guid.Parse("ffffffff-ffff-7000-8000-000000000000")));
    }
}
