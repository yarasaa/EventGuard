using System.Net;
using System.Text.Json;
using EventGuard.Operations;
namespace EventGuard.Dashboard.Tests;
public sealed class ModuleFlowTests
{
    private static Dictionary<string,string?> Config() => new() { ["Dashboard:Modules:0:Key"]="snapshots",["Dashboard:Modules:0:Label"]="Selections <script>",
        ["Dashboard:Modules:0:Module"]="Snapshots",["Dashboard:Modules:0:ScopeId"]=new JobScope("a","app").Id };
    [Fact]
    public async Task Module_grant_is_separate_and_checked_before_querying()
    {
        await using var app=new DashboardFactory(overrides:Config()); using var client=app.Client();
        Assert.Equal(HttpStatusCode.NotFound,(await client.GetAsync("/api/modules?target=snapshots")).StatusCode); Assert.Equal(0,app.Store.ModuleCalls);
        client.DefaultRequestHeaders.Add("Test-Module-Permission","test-store/snapshots");
        Assert.Contains("Selections &lt;script&gt;",await client.GetStringAsync("/modules?target=snapshots")); Assert.Equal(1,app.Store.ModuleCalls);
        using var anonymous=app.Client(null); Assert.Equal(HttpStatusCode.Unauthorized,(await anonymous.GetAsync("/api/modules")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound,(await client.GetAsync("/api/modules?target=foreign")).StatusCode);
    }
    [Fact]
    public async Task Signed_cursors_bind_filters_and_support_previous_page_without_leaking_errors()
    {
        await using var app=new DashboardFactory(overrides:Config()); using var client=app.Client(); client.DefaultRequestHeaders.Add("Test-Module-Permission","test-store/snapshots");
        using var first=JsonDocument.Parse(await client.GetStringAsync("/api/modules?target=snapshots&state=0")); var next=first.RootElement.GetProperty("next").GetString();
        using var second=JsonDocument.Parse(await client.GetStringAsync("/api/modules?target=snapshots&state=0&cursor="+Uri.EscapeDataString(next!))); var previous=second.RootElement.GetProperty("previous").GetString();
        Assert.NotNull(previous); Assert.Equal(HttpStatusCode.OK,(await client.GetAsync("/api/modules?target=snapshots&state=0&cursor="+Uri.EscapeDataString(previous!))).StatusCode); Assert.True(app.Store.LastModuleQuery!.Reverse);
        Assert.Equal(HttpStatusCode.BadRequest,(await client.GetAsync("/api/modules?target=snapshots&state=1&cursor="+Uri.EscapeDataString(next!))).StatusCode);
        app.Store.Unavailable=true; var failed=await client.GetAsync("/modules?target=snapshots"); Assert.Equal(HttpStatusCode.ServiceUnavailable,failed.StatusCode); Assert.DoesNotContain("Password",await failed.Content.ReadAsStringAsync());
    }
}
public sealed partial class MetadataStore : IModuleReadStore
{
    public int ModuleCalls; public ModuleQuery? LastModuleQuery;
    public Task<ModulePage> ReadModuleAsync(ModuleQuery query,CancellationToken cancellationToken=default)
    {
        ModuleCalls++; LastModuleQuery=query; if(Unavailable) throw new InvalidOperationException("Password=secret");
        return Task.FromResult(new ModulePage([new(WorkId,"Pending",0,0,0,"Waiting for source",DateTimeOffset.UtcNow)],WorkId,query.Position.HasValue?WorkId:null));
    }
}
