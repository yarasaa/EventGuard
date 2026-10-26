using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class ExternalMigrationTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Schema_one_upgrade_preserves_old_intents_without_adding_result_work()
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var stream = typeof(SqlServerWorkStore).Assembly.GetManifestResourceStream("EventGuard.SqlServer.External.Migrations.001_external.sql")!;
        using var reader = new StreamReader(stream);
        await using var command = new SqlCommand((await reader.ReadToEndAsync()).Replace("__SCHEMA__", db.Schema, StringComparison.Ordinal), connection);
        await command.ExecuteNonQueryAsync();
        var action = new ExternalAction(new("tenant", "legacy"), Guid.NewGuid(), "service", "v1", [1]);
        command.CommandText = $"""
            INSERT INTO {db.Schema}.external_actions(scope_id,id,tenant,producer,binding_name,binding_version,payload,fingerprint)
            VALUES(@scope,@id,@tenant,@producer,@binding,@bindingVersion,@payload,@fingerprint)
            """;
        command.Parameters.AddWithValue("scope", action.Scope.Id); command.Parameters.AddWithValue("id", action.Id);
        command.Parameters.AddWithValue("tenant", action.Scope.Tenant); command.Parameters.AddWithValue("producer", action.Scope.Producer);
        command.Parameters.AddWithValue("binding", action.Binding); command.Parameters.AddWithValue("bindingVersion", action.BindingVersion);
        command.Parameters.AddWithValue("payload", action.CopyPayload());
        command.Parameters.AddWithValue("fingerprint", StableHash.Of("external/origin/v1", action.Fingerprint, "", ""));
        await command.ExecuteNonQueryAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Store.ValidateExternalActionsAsync());
        await db.Store.InitializeExternalActionsAsync(); await db.Store.InitializeExternalActionsAsync();
        await db.Store.ValidateExternalActionsAsync();
        Assert.Equal(action.Fingerprint, (await db.Store.FindActionAsync(action.Scope, action.Id))!.Action.Fingerprint);
        var claim = (await db.Store.ClaimActionAsync(action.Scope, action.Binding, action.BindingVersion, ExternalPhase.Submit,
            Guid.NewGuid(), TimeSpan.FromSeconds(30)))!;
        await db.Store.SettleActionAsync(claim, new(ExternalResultKind.Accepted, "accepted"), new());
        command.Parameters.Clear(); command.CommandText = $"SELECT COUNT(*) FROM {db.Schema}.external_result_updates";
        Assert.Equal(0, Convert.ToInt32(await command.ExecuteScalarAsync()));
    }
}
