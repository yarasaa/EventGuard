using Confluent.Kafka;

namespace EventGuard.Kafka;

/// <summary>Serial durable acceptance followed by an explicit commit of only that partition's next offset.</summary>
public sealed class KafkaIngress
{
    private readonly ConsumerConfig _configuration;
    private readonly IWorkStore _store;
    private readonly WorkScope _scope;
    private readonly IdentityPolicy _policy;
    private readonly string[] _topics;
    private readonly Func<ConsumeResult<byte[], byte[]>, WorkSubmission> _map;
    private readonly TimeSpan _acceptTimeout;
    private readonly IngressQuarantineOptions? _quarantine;
    private readonly IIngressQuarantineStore? _quarantineStore;
    private int _running;

    public KafkaIngress(ConsumerConfig configuration, IWorkStore store, WorkScope scope, IdentityPolicy policy,
        IEnumerable<string> topics, Func<ConsumeResult<byte[], byte[]>, WorkSubmission>? map = null, TimeSpan? acceptanceTimeout = null,
        IngressQuarantineOptions? quarantine = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _quarantine = quarantine;
        _quarantineStore = quarantine is null ? null : store as IIngressQuarantineStore
            ?? throw new ArgumentException("Opt-in quarantine requires a durable ingress quarantine store.", nameof(store));
        _scope = scope ?? throw new ArgumentNullException(nameof(scope));
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        ArgumentNullException.ThrowIfNull(topics);
        _topics = topics.Distinct(StringComparer.Ordinal).ToArray();
        if (_topics.Length is < 1 or > 256 || _topics.Any(t => t is null || !KafkaEnvelope.ValidTopic(t)))
            throw new ArgumentException("Specify 1–256 concrete Kafka topic names; regex subscriptions are not supported.", nameof(topics));
        _configuration = new(configuration.ToDictionary(p => p.Key, p => p.Value));
        ArgumentException.ThrowIfNullOrWhiteSpace(_configuration.BootstrapServers);
        ArgumentException.ThrowIfNullOrWhiteSpace(_configuration.GroupId);
        if (_configuration.EnableAutoCommit == true || _configuration.EnableAutoOffsetStore == true)
            throw new ArgumentException("Automatic offset commit/storage would bypass durable acceptance.", nameof(configuration));
        if (_configuration.AutoOffsetReset is null) throw new ArgumentException("Choose an explicit initial/invalid offset policy.", nameof(configuration));
        if (_configuration.IsolationLevel is not (null or IsolationLevel.ReadCommitted))
            throw new ArgumentException("Only read_committed input is supported.", nameof(configuration));
        if (_configuration.GroupProtocol is not (null or GroupProtocol.Classic)
            || _configuration.PartitionAssignmentStrategy is not (null or PartitionAssignmentStrategy.Range or PartitionAssignmentStrategy.RoundRobin)
            || _configuration.GroupInstanceId is not null)
            throw new ArgumentException("The initial profile uses dynamic classic groups with eager range/roundrobin assignment.", nameof(configuration));
        if (_configuration.AllowAutoCreateTopics == true) throw new ArgumentException("Provision topics explicitly.", nameof(configuration));
        _configuration.EnableAutoCommit = false;
        _configuration.EnableAutoOffsetStore = false;
        _configuration.AllowAutoCreateTopics = false;
        _configuration.IsolationLevel = IsolationLevel.ReadCommitted;
        _configuration.GroupProtocol = GroupProtocol.Classic;
        _configuration.PartitionAssignmentStrategy ??= PartitionAssignmentStrategy.Range;
        _configuration.MaxPollIntervalMs ??= 300000;
        _configuration.QueuedMinMessages ??= 16;
        _configuration.QueuedMaxMessagesKbytes ??= 8192;
        _configuration.FetchMaxBytes ??= 2097152;
        _configuration.MaxPartitionFetchBytes ??= 2097152;
        _acceptTimeout = acceptanceTimeout ?? TimeSpan.FromSeconds(30);
        if (_acceptTimeout <= TimeSpan.Zero || _acceptTimeout.TotalMilliseconds >= _configuration.MaxPollIntervalMs)
            throw new ArgumentOutOfRangeException(nameof(acceptanceTimeout), "Acceptance timeout must be shorter than max.poll.interval.ms.");
        _map = map ?? (delivery => KafkaEnvelope.Map(delivery, _scope, _policy));
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0) throw new InvalidOperationException("This ingress is already running.");
        try { await Task.Run(() => RunLoopAsync(cancellationToken), CancellationToken.None).ConfigureAwait(false); }
        finally { Volatile.Write(ref _running, 0); }
    }

    private async Task RunLoopAsync(CancellationToken cancellationToken)
    {
        var assigned = new HashSet<TopicPartition>();
        Error? fatal = null;
        using var consumer = new ConsumerBuilder<byte[], byte[]>(_configuration)
            .SetLogHandler((_, _) => { })
            .SetErrorHandler((_, error) => { if (error.IsFatal) fatal = error; })
            .SetPartitionsAssignedHandler((_, partitions) => { assigned.Clear(); assigned.UnionWith(partitions); })
            .SetPartitionsRevokedHandler((_, _) => assigned.Clear())
            .SetPartitionsLostHandler((_, _) => assigned.Clear())
            .Build();
        var cleanStop = false;
        try
        {
            consumer.Subscribe(_topics);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (fatal is not null) throw new KafkaException(fatal);
                var delivery = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (delivery is null || delivery.IsPartitionEOF) continue;
                if (!assigned.Contains(delivery.TopicPartition)) throw new InvalidOperationException("Kafka delivered outside the current assignment.");
                // Native ConsumeResult is mutable. A mapper must never be able to change the settlement cursor.
                var nextOffset = new TopicPartitionOffset(delivery.TopicPartition, delivery.Offset + 1);
                if (delivery.Message.Value?.Length > 1024 * 1024) throw new InvalidOperationException("Ingress inline payload exceeds 1 MiB.");
                var snapshot = _quarantine is null ? null : KafkaIngressSnapshot.Capture(delivery, _quarantine);
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                deadline.CancelAfter(_acceptTimeout);
                WorkSubmission? submission = null;
                IngressMessageRejectedException? rejected = null;
                try { submission = _map(delivery) ?? throw new InvalidOperationException("Ingress mapper returned no submission."); }
                catch (IngressMessageRejectedException error) when (snapshot is not null) { rejected = error; }
                if (rejected is not null)
                {
                    var quarantined = await _quarantineStore!.QuarantineAsync(_scope, _policy, snapshot!, rejected.ReasonCode, deadline.Token).ConfigureAwait(false);
                    if (quarantined is null || quarantined.Status is not (IngressQuarantineStatus.Recorded or IngressQuarantineStatus.Duplicate))
                        throw new InvalidOperationException("Ingress quarantine did not durably preserve this input.");
                }
                else
                {
                    if (submission!.Identity.Scope != _scope || submission.Identity.Policy != _policy)
                        throw new InvalidOperationException("Ingress mapping changed its configured scope or identity policy.");
                    var result = await _store.AcceptAsync(submission, deadline.Token).ConfigureAwait(false);
                    if (result is null || !Enum.IsDefined(result.Status)) throw new InvalidOperationException("Store returned no durable acceptance outcome.");
                }
                deadline.Token.ThrowIfCancellationRequested();
                // No next Consume call occurs until this acceptance and explicit per-partition commit finish.
                // Native group generation validation fences a member that lost ownership while awaiting DB IO.
                consumer.Commit([nextOffset]);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { cleanStop = true; }
        finally
        {
            // Close in 2.15.1 can block in RebalanceProtocol while draining a lost-assignment
            // callback. Unsubscribe while the group is live, then let using dispose the native
            // handle without draining rebalance callbacks. Neither operation commits offsets.
            // Group reassignment can take up to the configured session timeout.
            try { consumer.Unsubscribe(); }
            catch (KafkaException) when (!cleanStop) { /* Preserve the original acceptance/commit failure. */ }
        }
    }
}
