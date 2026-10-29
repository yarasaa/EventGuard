using System.Reflection;
using System.Xml.Linq;
using EventGuard.PostgreSql;
using EventGuard.Runtime;
using EventGuard.RabbitMq;
using EventGuard.SqlServer;
using EventGuard.Kafka;

namespace EventGuard.Architecture.Tests;

public sealed class DependencyRulesTests
{
    // Adding a production project requires an explicit architectural dependency decision.
    private static readonly Dictionary<string, Boundary> Boundaries = new(StringComparer.Ordinal)
    {
        ["EventGuard.Operations.Prometheus"] = new(typeof(EventGuard.Operations.Prometheus.PrometheusWorkerMonitor).Assembly, ["EventGuard.Operations"], [], ["EventGuard.Operations"]),
        ["EventGuard.Kafka"] = new(typeof(KafkaPublisher).Assembly, ["EventGuard.Core"], ["Confluent.Kafka"], ["EventGuard.Core", "Confluent.Kafka"]),
        ["EventGuard.Operations"] = new(typeof(EventGuard.Operations.OperationsReader).Assembly, ["EventGuard.Core"], [], ["EventGuard.Core"]),
        ["EventGuard.Operations.PostgreSql"] = new(typeof(EventGuard.Operations.PostgreSql.PostgreSqlWorkReader).Assembly,
            ["EventGuard.Operations"], ["Npgsql"], ["EventGuard.Core", "EventGuard.Operations", "Npgsql"]),
        ["EventGuard.Operations.SqlServer"] = new(typeof(EventGuard.Operations.SqlServer.SqlServerWorkReader).Assembly,
            ["EventGuard.Operations"], ["Microsoft.Data.SqlClient", "System.Security.Cryptography.Pkcs"], ["EventGuard.Core", "EventGuard.Operations", "Microsoft.Data.SqlClient"]),
        ["EventGuard.Operations.Replay.PostgreSql"] = new(typeof(EventGuard.Operations.Replay.PostgreSql.PostgreSqlReplayStore).Assembly,
            ["EventGuard.Operations", "EventGuard.PostgreSql"], ["Npgsql"], ["EventGuard.Core", "EventGuard.Operations", "EventGuard.PostgreSql", "Npgsql"]),
        ["EventGuard.Operations.Replay.SqlServer"] = new(typeof(EventGuard.Operations.Replay.SqlServer.SqlServerReplayStore).Assembly,
            ["EventGuard.Operations", "EventGuard.SqlServer"], ["Microsoft.Data.SqlClient", "System.Security.Cryptography.Pkcs"], ["EventGuard.Core", "EventGuard.Operations", "EventGuard.SqlServer", "Microsoft.Data.SqlClient"]),
        ["EventGuard.Dashboard"] = new(typeof(EventGuard.Dashboard.DashboardQueries).Assembly,
            ["EventGuard.Operations", "EventGuard.Operations.Prometheus", "EventGuard.Operations.PostgreSql", "EventGuard.Operations.SqlServer", "EventGuard.Operations.Replay.PostgreSql", "EventGuard.Operations.Replay.SqlServer"],
            ["Microsoft.AspNetCore.Authentication.OpenIdConnect", "prometheus-net"],
            ["EventGuard.Core", "EventGuard.Operations", "EventGuard.Operations.Prometheus", "EventGuard.Operations.PostgreSql", "EventGuard.Operations.SqlServer", "EventGuard.Operations.Replay.PostgreSql", "EventGuard.Operations.Replay.SqlServer", "Microsoft.IdentityModel.Tokens", "Prometheus.NetStandard"]),
        ["EventGuard.Hosting"] = new(typeof(EventGuard.Hosting.EventGuardBuilder).Assembly, ["EventGuard.Runtime"], ["Microsoft.Extensions.Hosting"], ["EventGuard.Core", "EventGuard.Runtime", "Microsoft.Extensions.Hosting", "Microsoft.Extensions.Hosting.Abstractions", "Microsoft.Extensions.DependencyInjection.Abstractions", "Microsoft.Extensions.Logging.Abstractions", "Microsoft.Extensions.Configuration.Abstractions", "Microsoft.Extensions.Options"]),
        ["EventGuard.EntityFrameworkCore"] = new(typeof(EventGuard.EntityFrameworkCore.EventGuardEfCore).Assembly, ["EventGuard.Core"], ["Microsoft.EntityFrameworkCore.Relational"], ["EventGuard.Core", "Microsoft.EntityFrameworkCore", "Microsoft.EntityFrameworkCore.Relational"]),
        ["EventGuard.Hosting.Kafka"] = new(typeof(EventGuard.Hosting.KafkaHostingExtensions).Assembly, ["EventGuard.Hosting", "EventGuard.Kafka"], [], ["EventGuard.Core", "EventGuard.Hosting", "EventGuard.Kafka", "Confluent.Kafka", "Microsoft.Extensions.DependencyInjection.Abstractions"]),
        ["EventGuard.Hosting.RabbitMq"] = new(typeof(EventGuard.Hosting.RabbitMqHostingExtensions).Assembly, ["EventGuard.Hosting", "EventGuard.RabbitMq"], [], ["EventGuard.Core", "EventGuard.Hosting", "EventGuard.RabbitMq", "RabbitMQ.Client", "Microsoft.Extensions.DependencyInjection.Abstractions"]),
        ["EventGuard.Core"] = new(typeof(WorkIdentity).Assembly, [], [], []),
        ["EventGuard.Runtime"] = new(typeof(WorkPump<>).Assembly, ["EventGuard.Core"], [], ["EventGuard.Core"]),
        ["EventGuard.PostgreSql"] = new(typeof(PostgreSqlWorkStore).Assembly, ["EventGuard.Core"], ["Npgsql"], ["EventGuard.Core", "Npgsql"]),
        ["EventGuard.RabbitMq"] = new(typeof(RabbitMqPublisher).Assembly, ["EventGuard.Core"], ["RabbitMQ.Client"], ["EventGuard.Core", "RabbitMQ.Client"]),
        ["EventGuard.SqlServer"] = new(typeof(SqlServerWorkStore).Assembly, ["EventGuard.Core"], ["Microsoft.Data.SqlClient", "System.Security.Cryptography.Pkcs"], ["EventGuard.Core", "Microsoft.Data.SqlClient"])
    };

    [Fact]
    public void Every_production_project_has_an_explicit_boundary_and_is_in_the_solution()
    {
        var root = RepositoryRoot();
        var projects = Directory.GetFiles(Path.Combine(root, "src"), "*.csproj", SearchOption.AllDirectories);
        Assert.Equal(Boundaries.Keys.Order(), projects.Select(Path.GetFileNameWithoutExtension).Order());
        var solution = XDocument.Load(Path.Combine(root, "EventGuard.slnx"));
        var included = solution.Descendants("Project").Select(p => Path.GetFullPath(Path.Combine(root, p.Attribute("Path")!.Value))).ToHashSet();
        Assert.All(projects, path => Assert.Contains(path, included));
    }

    [Theory]
    [InlineData("EventGuard.Operations.Prometheus")]
    [InlineData("EventGuard.Hosting")]
    [InlineData("EventGuard.Hosting.Kafka")]
    [InlineData("EventGuard.Hosting.RabbitMq")]
    [InlineData("EventGuard.EntityFrameworkCore")]
    [InlineData("EventGuard.Core")]
    [InlineData("EventGuard.Runtime")]
    [InlineData("EventGuard.PostgreSql")]
    [InlineData("EventGuard.RabbitMq")]
    [InlineData("EventGuard.SqlServer")]
    [InlineData("EventGuard.Kafka")]
    [InlineData("EventGuard.Operations")]
    [InlineData("EventGuard.Operations.PostgreSql")]
    [InlineData("EventGuard.Operations.SqlServer")]
    [InlineData("EventGuard.Dashboard")]
    [InlineData("EventGuard.Operations.Replay.PostgreSql")]
    [InlineData("EventGuard.Operations.Replay.SqlServer")]
    public void Declared_project_and_package_dependencies_stay_inside_the_boundary(string project)
    {
        var boundary = Boundaries[project];
        var definition = XDocument.Load(Path.Combine(RepositoryRoot(), "src", project, project + ".csproj"));
        var references = definition.Descendants("ProjectReference").Select(p => Path.GetFileNameWithoutExtension(p.Attribute("Include")!.Value));
        var packages = definition.Descendants("PackageReference").Select(p => p.Attribute("Include")!.Value);
        Assert.All(references, name => Assert.Contains(name, boundary.Projects));
        Assert.All(packages, name => Assert.Contains(name, boundary.Packages));
        Assert.Empty(definition.Descendants("FrameworkReference"));
        Assert.Empty(definition.Descendants("Reference"));
    }

    [Theory]
    [InlineData("EventGuard.Operations.Prometheus")]
    [InlineData("EventGuard.Hosting")]
    [InlineData("EventGuard.Hosting.Kafka")]
    [InlineData("EventGuard.Hosting.RabbitMq")]
    [InlineData("EventGuard.EntityFrameworkCore")]
    [InlineData("EventGuard.Core")]
    [InlineData("EventGuard.Runtime")]
    [InlineData("EventGuard.PostgreSql")]
    [InlineData("EventGuard.RabbitMq")]
    [InlineData("EventGuard.SqlServer")]
    [InlineData("EventGuard.Kafka")]
    [InlineData("EventGuard.Operations")]
    [InlineData("EventGuard.Operations.PostgreSql")]
    [InlineData("EventGuard.Operations.SqlServer")]
    [InlineData("EventGuard.Dashboard")]
    [InlineData("EventGuard.Operations.Replay.PostgreSql")]
    [InlineData("EventGuard.Operations.Replay.SqlServer")]
    public void Compiled_dependencies_do_not_pull_host_ui_or_other_adapters_inward(string project)
    {
        var boundary = Boundaries[project];
        foreach (var reference in boundary.Assembly.GetReferencedAssemblies())
        {
            var name = reference.Name!;
            var framework = name.StartsWith("System.", StringComparison.Ordinal) || name is "System" or "netstandard" or "mscorlib";
            var hostFramework = project == "EventGuard.Dashboard" &&
                (name == "Microsoft.AspNetCore" || name.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal) || name.StartsWith("Microsoft.Extensions.", StringComparison.Ordinal));
            Assert.True(framework || hostFramework || boundary.Assemblies.Contains(name), $"{project} unexpectedly references {name}.");
            if (project is "EventGuard.Core" or "EventGuard.Runtime" or "EventGuard.Operations")
                Assert.False(name.StartsWith("System.Data", StringComparison.Ordinal), $"{project} must not depend on database types.");
        }
    }

    [Fact]
    public void Execution_loop_is_optional_and_does_not_live_in_core()
    {
        Assert.DoesNotContain(typeof(WorkIdentity).Assembly.GetExportedTypes(), t => t.Name.StartsWith("WorkPump", StringComparison.Ordinal));
        Assert.Equal("EventGuard.Runtime", typeof(WorkPump<>).Assembly.GetName().Name);
        Assert.Equal("EventGuard.Runtime", typeof(WorkPump<>).Namespace);
    }

    private static string RepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "EventGuard.slnx"))) directory = directory.Parent;
        return directory?.FullName ?? throw new InvalidOperationException("Run architectural tests from the repository checkout.");
    }

    private sealed record Boundary(Assembly Assembly, string[] Projects, string[] Packages, string[] Assemblies);
}
