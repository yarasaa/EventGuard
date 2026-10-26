namespace EventGuard;

// Pure decisions. Providers must read under native locks, use their durable clock and
// persist each change with its attempt/request record in the SAME transaction.
// A decision is neither a lock, an authorization grant, nor permission to rerun a handler.
public static class WorkLifecycle
{
    public static void ValidateClaimRequest(Guid requestId, TimeSpan leaseDuration)
    {
        if (requestId == Guid.Empty) throw new ArgumentException("A stable claim request id is required.", nameof(requestId));
        if (leaseDuration <= TimeSpan.Zero || leaseDuration > TimeSpan.FromHours(1))
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));
    }

    public static ClaimDecision? Claim(WorkItem work, WorkLease? ownership, Guid requestId,
        TimeSpan leaseDuration, DateTimeOffset now)
    {
        Validate(work);
        ValidateClaimRequest(requestId, leaseDuration);
        if ((work.State == WorkState.Processing) != (ownership is not null) || ownership?.Token == Guid.Empty)
            throw new InvalidOperationException("Durable state and ownership are inconsistent.");
        var eligible = work.State switch
        {
            WorkState.Pending or WorkState.RetryScheduled => work.NextAttemptAt <= now || work.ExpiresAt <= now,
            WorkState.Processing => ownership!.Until <= now,
            _ => false
        };
        if (!eligible) return null;
        now = now.ToUniversalTime();
        var after = work with
        {
            State = WorkState.Processing,
            Version = checked(work.Version + 1),
            AttemptCount = checked(work.AttemptCount + 1),
            GenerationAttempt = checked(work.GenerationAttempt + 1)
        };
        return new(new(work, after), new(requestId, now + leaseDuration), ownership?.Token);
    }

    public static ExecutionAdmission AdmitExecution(WorkItem work, WorkLease? ownership, Guid token, DateTimeOffset now)
    {
        Validate(work);
        if (!Owns(work, ownership, token) || ownership!.Until <= now) return ExecutionAdmission.LeaseLost;
        return work.ExpiresAt <= now ? ExecutionAdmission.Expire : ExecutionAdmission.Execute;
    }

    // Called after the handler succeeded while the provider retained its native lock.
    // An elapsed lease/expiry does not invalidate an execution that already began under that lock.
    public static ExecutionDecision Complete(WorkItem work, WorkLease? ownership, Guid token, DateTimeOffset now)
    {
        RequireOwner(work, ownership, token);
        return Finish(work, WorkState.Completed, ExecutionStatus.Completed, now, null, "Completed");
    }

    public static ExecutionDecision Expire(WorkItem work, WorkLease? ownership, Guid token, DateTimeOffset now)
    {
        RequireOwner(work, ownership, token);
        if (work.ExpiresAt is null || work.ExpiresAt > now) throw new InvalidOperationException("Work has not expired.");
        return Finish(work, WorkState.Expired, ExecutionStatus.Expired, now, "expired", "Expired");
    }

    // Only after rollback was confirmed and ownership was read again under a new native lock.
    public static ExecutionDecision? Fail(WorkItem work, WorkLease? ownership, Guid token,
        RetryPolicy policy, FailureKind kind, DateTimeOffset now, double randomSample)
    {
        Validate(work);
        ArgumentNullException.ThrowIfNull(policy);
        if (!Owns(work, ownership, token)) return null;
        var retry = policy.Decide(kind, work.GenerationAttempt, now.ToUniversalTime(), randomSample);
        return retry.IsTerminal
            ? Finish(work, WorkState.DeadLetter, ExecutionStatus.DeadLetter, retry.NextAttemptAt, retry.ReasonCode, "Failed")
            : Finish(work, WorkState.RetryScheduled, ExecutionStatus.RetryScheduled, retry.NextAttemptAt, retry.ReasonCode, "Failed");
    }

    // Look up a request receipt before evaluating today's work state. A repeated request
    // must return its old outcome even when the work has failed again in a later attempt.
    public static ReplayResult RepeatReplay(ReplayRequest request, string recordedFingerprint, ReplayResult recordedResult)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(recordedFingerprint);
        ArgumentNullException.ThrowIfNull(recordedResult);
        if (recordedFingerprint != request.Fingerprint) return new(ReplayStatus.RequestConflict, request.WorkId, 0, 0);
        if (recordedResult.WorkId != request.WorkId) throw new InvalidOperationException("Replay receipt does not match its fingerprint.");
        return recordedResult with { RepeatedRequest = true };
    }

    public static ReplayDecision Replay(ReplayRequest request, WorkItem? work, DateTimeOffset now) => Replay(request, work, now, true);

    public static ReplayDecision Replay(ReplayRequest request, WorkItem? work, DateTimeOffset now, bool payloadAvailable)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (work is not null && (work.Id != request.WorkId || work.ScopeId != request.Scope.Id)) work = null;
        if (work is not null) Validate(work);
        var status = work switch
        {
            null => ReplayStatus.NotFound,
            { State: not WorkState.DeadLetter } => ReplayStatus.NotFailed,
            _ when work.Version != request.ExpectedVersion => ReplayStatus.VersionConflict,
            _ when !payloadAvailable || work.ExpiresAt <= now => ReplayStatus.PayloadExpired,
            _ => ReplayStatus.Accepted
        };
        if (status != ReplayStatus.Accepted)
            return new(new(status, request.WorkId, work?.ReplayGeneration ?? 0, work?.Version ?? 0), null);
        var before = work!;
        var after = before with
        {
            State = WorkState.Pending,
            Version = checked(before.Version + 1),
            ReplayGeneration = checked(before.ReplayGeneration + 1),
            GenerationAttempt = 0,
            NextAttemptAt = now.ToUniversalTime(),
            LastReason = "replay-requested"
        };
        return new(new(status, after.Id, after.ReplayGeneration, after.Version), new(before, after));
    }

    private static ExecutionDecision Finish(WorkItem work, WorkState state, ExecutionStatus status,
        DateTimeOffset next, string? reason, string attemptStatus) =>
        new(new(work, work with
        {
            State = state,
            Version = checked(work.Version + 1),
            NextAttemptAt = next.ToUniversalTime(),
            LastReason = reason
        }), status, attemptStatus);

    private static bool Owns(WorkItem work, WorkLease? ownership, Guid token) =>
        token != Guid.Empty && work.State == WorkState.Processing && ownership?.Token == token;

    private static void RequireOwner(WorkItem work, WorkLease? ownership, Guid token)
    {
        Validate(work);
        if (!Owns(work, ownership, token)) throw new InvalidOperationException("The attempt no longer owns this work.");
    }

    private static void Validate(WorkItem work)
    {
        ArgumentNullException.ThrowIfNull(work);
        if (!Enum.IsDefined(work.State) || work.Version < 0 || work.AttemptCount < 0 || work.GenerationAttempt < 0
            || work.GenerationAttempt > work.AttemptCount || work.ReplayGeneration < 0)
            throw new InvalidOperationException("Invalid durable work snapshot.");
    }
}
