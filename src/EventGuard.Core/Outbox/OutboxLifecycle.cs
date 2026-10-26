namespace EventGuard;

public static class OutboxLifecycle
{
    public static ProducerStatus ExistingProducer(ProducerRequest request, string recordedFingerprint)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(recordedFingerprint);
        return request.Fingerprint.Value == recordedFingerprint ? ProducerStatus.AlreadyCommitted : ProducerStatus.RequestConflict;
    }

    // Candidate read and these changes are applied under the provider's native row lock.
    public static OutboxRecord? Claim(OutboxRecord record, WorkLease? ownership, DateTimeOffset now)
    {
        Validate(record);
        if ((record.State == OutboxState.Publishing) != (ownership is not null))
            throw new InvalidOperationException("Outbox state and ownership are inconsistent.");
        if (ownership?.Token == Guid.Empty) throw new InvalidOperationException("Outbox lease token is empty.");
        var eligible = record.State switch
        {
            OutboxState.Pending or OutboxState.RetryScheduled => record.NextAttemptAt <= now,
            OutboxState.Publishing => ownership!.Until <= now,
            _ => false
        };
        if (!eligible) return null;
        return record with
        {
            State = OutboxState.Publishing,
            Version = checked(record.Version + 1),
            AttemptCount = checked(record.AttemptCount + 1),
            GenerationAttempt = checked(record.GenerationAttempt + 1),
            DeliveryUncertain = record.DeliveryUncertain || ownership is not null,
            LastReason = ownership is not null ? "publish-interrupted" : record.LastReason
        };
    }

    // Unlike business execution, publishing happens OUTSIDE the DB transaction.
    // An expired lease can allow duplicate sends. Fencing protects state, not the broker side effect.
    public static OutboxRecord Settle(OutboxRecord record, PublishOutcome outcome, RetryPolicy policy,
        DateTimeOffset now, double randomSample)
    {
        Validate(record);
        ArgumentNullException.ThrowIfNull(outcome);
        ArgumentNullException.ThrowIfNull(policy);
        if (record.State != OutboxState.Publishing || record.GenerationAttempt < 1)
            throw new InvalidOperationException("Outbox is not publishing an active attempt.");
        var uncertain = record.DeliveryUncertain || outcome.Status == PublishStatus.Unknown;
        if (outcome.Status == PublishStatus.Accepted)
            return record with
            {
                State = OutboxState.Published,
                Version = checked(record.Version + 1),
                DeliveryUncertain = uncertain,
                LastReason = outcome.ReasonCode
            };
        var retry = policy.Decide(outcome.Status == PublishStatus.NotSentPermanent ? FailureKind.Permanent : FailureKind.Transient,
            record.GenerationAttempt, now.ToUniversalTime(), randomSample);
        return record with
        {
            State = retry.IsTerminal ? OutboxState.DeadLetter : OutboxState.RetryScheduled,
            Version = checked(record.Version + 1),
            NextAttemptAt = retry.NextAttemptAt,
            DeliveryUncertain = uncertain,
            LastReason = outcome.ReasonCode
        };
    }

    private static void Validate(OutboxRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (record.Message is null || !Enum.IsDefined(record.State) || record.Version < 0 || record.AttemptCount < 0
            || record.GenerationAttempt < 0 || record.GenerationAttempt > record.AttemptCount || record.ReplayGeneration < 0)
            throw new InvalidOperationException("Invalid outbox record.");
    }
}
