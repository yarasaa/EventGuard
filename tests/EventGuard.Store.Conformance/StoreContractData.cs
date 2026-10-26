namespace EventGuard.Store.Conformance;

public static class StoreContractData
{
    public static WorkScope Scope() => new("tenant", "files-" + Guid.NewGuid().ToString("N"), "sample", "file.created");

    public static WorkSubmission Submission(WorkScope scope, string key = "file-1", byte[]? payload = null,
        string version = "v1", DateTimeOffset? notBefore = null, DateTimeOffset? expiresAt = null)
    {
        payload ??= "{\"file\":\"sample.txt\"}"u8.ToArray();
        return new(new(scope, new(version, "sha256-bytes/v1"), key), Guid.NewGuid(), payload,
            ContentFingerprint.FromBytes(payload), notBefore, expiresAt);
    }
}
