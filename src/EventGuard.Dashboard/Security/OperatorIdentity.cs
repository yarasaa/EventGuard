using System.Security.Claims;

namespace EventGuard.Dashboard;

internal static class OperatorIdentity
{
    public static string? Actor(ClaimsPrincipal user)
    {
        var subjects = user.FindAll("sub").ToArray();
        var issuers = user.FindAll("iss").ToArray();
        if (subjects.Length != 1 || issuers.Length != 1 || string.IsNullOrWhiteSpace(subjects[0].Value)
            || string.IsNullOrWhiteSpace(issuers[0].Value) || subjects[0].Value.Length > 1024 || issuers[0].Value.Length > 2048) return null;
        return StableHash.Of("eventguard-operator/v1", issuers[0].Value, subjects[0].Value);
    }

}
