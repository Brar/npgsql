using System;
using System.Collections.Generic;
using System.Net.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.Logging;
using Npgsql.Replication.Internal;
using Npgsql.Replication.Logical;
using Npgsql.TypeHandlers.DateTimeHandlers;
using static Npgsql.Util.Statics;

namespace Npgsql.Replication
{
    /// <summary>
    /// Defines the core behavior of replication connections and provides the base class for
    /// <see cref="NpgsqlLogicalReplicationConnection"/> and
    /// <see cref="Npgsql.Replication.Physical.NpgsqlPhysicalReplicationConnection"/>.
    /// </summary>
    public abstract class NpgsqlReplicationConnection : IDisposable, IAsyncDisposable
    {
        #region Fields

        internal NpgsqlConnection Connection = default!;
        private protected ReplicationConnectionState State { get; set; }
        private protected readonly Timer FeedbackTimer;
        private protected readonly SemaphoreSlim FeedbackSemaphore = new SemaphoreSlim(1, 1);
        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(NpgsqlReplicationConnection));
        long _disposing;

        #endregion Fields

        /// <summary>
        /// Initializes a new instance of <see cref="NpgsqlReplicationConnection"/>.
        /// </summary>
        private protected NpgsqlReplicationConnection()
        {
            FeedbackTimer = new Timer(TimerSendFeedback);
        }

        #region Properties

        /// <summary>
        /// Gets or sets the string used to connect to a PostgreSQL database. See the manual for details.
        /// </summary>
        /// <value>The connection string that includes the server name,
        /// the database name, and other parameters needed to establish
        /// the initial connection. The default value is an empty string.
        /// </value>
#nullable disable
        public string ConnectionString { get; set; }
#nullable enable

        /// <summary>
        /// The location of the last WAL byte + 1 received in the standby.
        /// </summary>
        public ulong LastReceivedLsn { get; private protected set; }

        /// <summary>
        /// The location of the last WAL byte + 1 flushed to disk in the standby.
        /// </summary>
        public ulong LastFlushedLsn { get; set; }

        /// <summary>
        /// The location of the last WAL byte + 1 applied (e. g. written to disk) in the standby.
        /// </summary>
        public ulong LastAppliedLsn { get; set; }

        /// <summary>
        /// Send replies at least this often.
        /// Timeout.<see cref="Timeout.InfiniteTimeSpan"/> disables automated replies.
        /// </summary>
        public TimeSpan WalReceiverStatusInterval { get; set; } = TimeSpan.FromSeconds(10d);

        /// <summary>
        /// Time that receiver waits for communication from master.
        /// Timeout.<see cref="Timeout.InfiniteTimeSpan"/> disables the timeout.
        /// </summary>
        public TimeSpan WalReceiverTimeout { get; set; } = TimeSpan.FromSeconds(60d);

        #endregion Properties

        private protected async Task OpenAsync(NpgsqlConnectionStringBuilder settings, CancellationToken cancellationToken)
        {
            settings.Pooling = settings.Enlist = false;
            settings.ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading;
            // TODO: Keepalive

            Connection = new NpgsqlConnection(settings.ToString());
            await Connection.OpenAsync(cancellationToken);
            State = ReplicationConnectionState.Idle;
        }

        #region Replication commands

        /// <summary>
        /// Sends a status update to PostgreSQL with the given WAL tracking information.
        /// The information is recorded by Npgsql and will be used in subsequent periodic status updates.
        /// </summary>
        /// <param name="lastFlushedLsn">The location of the last WAL byte + 1 flushed to disk in the standby.</param>
        /// <param name="lastAppliedLsn">The location of the last WAL byte + 1 applied in the standby.</param>
        /// <param name="force">Force sending out the status update immediately. Otherwise this just updates the
        /// internal tracking of <paramref name="lastFlushedLsn"/> and <paramref name="lastAppliedLsn"/> and sends the
        /// status update on schedule at </param>
        /// <exception cref="InvalidOperationException"></exception>
        /// <remarks>
        /// This is the only method in <see cref="NpgsqlLogicalReplicationConnection"/> you can safely call from a
        /// separate thread to update the status.
        /// </remarks>
        /// <returns>A Task representing the sending fo the status update (and not any PostgreSQL response.</returns>
        [PublicAPI]
        public async Task SendStatusUpdate(ulong lastFlushedLsn = 0UL, ulong lastAppliedLsn = 0UL, bool force = false)
        {
            if (force && State != ReplicationConnectionState.Streaming)
                throw new InvalidOperationException("The connection must be streaming in order to send status updates");

            if (lastFlushedLsn > 0UL)
                LastFlushedLsn = lastFlushedLsn;

            if (lastAppliedLsn > 0UL)
                LastAppliedLsn = lastAppliedLsn;

            if (force && await FeedbackSemaphore.WaitAsync(Timeout.Infinite))
                await SendFeedback();
        }

        /// <summary>
        /// Requests the server to identify itself.
        /// </summary>
        [PublicAPI]
        public async Task<NpgsqlReplicationIdentificationInfo> IdentifySystem()
        {
            CheckReady();
            var results = await ReadSingleRow("IDENTIFY_SYSTEM");
            return new NpgsqlReplicationIdentificationInfo(
                (string)results[0],
                (int)results[1],
                (string)results[2],
                (string)results[3]);
        }

        /// <summary>
        /// Requests the server to send over the timeline history file for timeline tli.
        /// </summary>
        /// <returns></returns>
        public async Task<NpgsqlTimelineHistoryFile> TimelineHistory()
        {
            // ToDo: Implement
            await Task.Delay(0);
            throw new NotImplementedException();
        }

        /// <summary>
        /// Requests the server to send the current setting of a run-time parameter.
        /// This is similar to the SQL command SHOW.
        /// </summary>
        [PublicAPI]
        public async Task<string> Show(string parameterName)
            => (string)(await ReadSingleRow("SHOW " + parameterName))[0];

        internal static readonly Version TemporaryReplicationSlotSupportedVersion = new Version(10, 0);

        internal async Task<NpgsqlReplicationSlotInfo> CreateReplicationSlotInternal(string slotName,
            bool temporary, string createCommandSuffix)
        {
            var sb = new StringBuilder("CREATE_REPLICATION_SLOT ").Append(slotName);
            if (temporary)
                sb.Append(" TEMPORARY");

            sb.Append(createCommandSuffix);
            try
            {
                var results = await ReadSingleRow(sb.ToString());
                return new NpgsqlReplicationSlotInfo((string)results[0], (string)results[1], (string)results[2],
                    (string)results[3]);
            }
            catch (PostgresException e)
            {
                if (Connection.PostgreSqlVersion < TemporaryReplicationSlotSupportedVersion && e.SqlState == "42601" /* syntax_error */)
                {
                    if (temporary)
                        throw new ArgumentException($"Temporary replication slots were introduced in PostgreSQL {TemporaryReplicationSlotSupportedVersion.ToString(1)}. Using PostgreSQL version {Connection.PostgreSqlVersion.ToString(3)} you have to leave the {nameof(temporary)} argument as false.", nameof(temporary), e);
                }
                throw;
            }
        }

        internal async Task<IAsyncEnumerable<XLogData>> StartReplication(string startReplicationCommandText, bool bypassingStream)
        {
            var connector = Connection.Connector!;
            await connector.WriteQuery(startReplicationCommandText, true);
            await connector.Flush(true);

            var msg = await connector.ReadMessage(true);
            switch (msg.Code)
            {
            case BackendMessageCode.CopyBothResponse:
                State = ReplicationConnectionState.Streaming;
                FeedbackTimer.Change(WalReceiverStatusInterval, Timeout.InfiniteTimeSpan);
                break;
            case BackendMessageCode.CompletedResponse:
                // TODO: This can happen when the client requests streaming at exactly the end of an old timeline.
                // TODO: Figure out how to communicate these different states to the user
                throw new NotImplementedException();
            default:
                throw connector.UnexpectedMessageReceived(msg.Code);
            }

            return StartStreaming();

            async IAsyncEnumerable<XLogData> StartStreaming()
            {
                var buf = connector.ReadBuffer;
                NpgsqlReadBuffer.ColumnStream columnStream = new NpgsqlReadBuffer.ColumnStream(buf);

                while (true)
                {
                    try
                    {
                        msg = await connector.ReadMessage(true);
                    }
                    catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.QueryCanceled)
                    {
                        State = ReplicationConnectionState.Idle;
                        yield break;
                    }

                    if (msg.Code != BackendMessageCode.CopyData)
                        throw connector.UnexpectedMessageReceived(msg.Code);

                    var messageLength = ((CopyDataMessage)msg).Length;
                    await buf.EnsureAsync(1);
                    var code = (char)buf.ReadByte();
                    switch (code)
                    {
                    case 'w': // XLogData
                    {
                        await buf.EnsureAsync(24);
                        var startLsn = buf.ReadUInt64();
                        var endLsn = buf.ReadUInt64();
                        var sendTime = TimestampHandler.Int64ToNpgsqlDateTime(buf.ReadInt64()).ToDateTime();

                        if (LastReceivedLsn < startLsn)
                            LastReceivedLsn = startLsn;
                        if (LastReceivedLsn < endLsn)
                            LastReceivedLsn = endLsn;

                        // dataLen = msg.Length - (code = 1 + walStart = 8 + walEnd = 8 + serverClock = 8)
                        var dataLen = messageLength - 25;
                        columnStream.Init(dataLen, canSeek: false);
                        var data = new XLogData(startLsn, endLsn, sendTime, columnStream);

                        yield return data;

                        // Our consumer may have disposed the stream which isn't necessary but shouldn't hurt us
                        if (columnStream.IsDisposed)
                            columnStream = new NpgsqlReadBuffer.ColumnStream(buf);
                        // Our consumer may not have read the stream to the end, but it might as well have been us
                        // ourselves bypassing the stream and reading directly from the buffer in StartReplication()
                        else if (columnStream.Position < columnStream.Length && !bypassingStream)
                            await buf.Skip(columnStream.Length - columnStream.Position, true);

                        continue;
                    }

                    case 'k': // Primary keepalive message
                    {
                        await buf.EnsureAsync(17);
                        var endLsn = buf.ReadUInt64();
                        var timestamp = buf.ReadInt64();
                        var replyRequested = buf.ReadByte() == 1;
                        if (LastReceivedLsn < endLsn)
                            LastReceivedLsn = endLsn;
                        if (replyRequested && await FeedbackSemaphore.WaitAsync(Timeout.Infinite))
                            await SendFeedback();

                        continue;
                    }

                    default:
                        throw connector.Break(new NpgsqlException($"Unknown replication message code '{code}'"));
                    }
                }
            }
        }

        /// <summary>
        /// Drops a replication slot, freeing any reserved server-side resources.
        /// If the slot is a logical slot that was created in a database other than
        /// the database the walsender is connected to, this command fails.
        /// </summary>
        /// <param name="slotName">The name of the slot to drop.</param>
        /// <param name="wait">
        /// This option causes the command to wait if the slot is active until it becomes inactive,
        /// instead of the default behavior of raising an error.
        /// </param>
        [PublicAPI]
        public async Task DropReplicationSlot(string slotName, bool wait = false)
        {
            CheckReady();

            var command = "DROP_REPLICATION_SLOT " + slotName;
            if (wait)
                command += " WAIT";

            var connector = Connection.Connector!;
            await connector.WriteQuery(command, true);
            await connector.Flush(true);

            Expect<CommandCompleteMessage>(await connector.ReadMessage(true), connector);
            Expect<CommandCompleteMessage>(await connector.ReadMessage(true), connector);  // Two CommandComplete are returned
            Expect<ReadyForQueryMessage>(await connector.ReadMessage(true), connector);
        }

        /// <summary>
        /// Stops an in-progress replication.
        /// </summary>
        [PublicAPI]
        public void Cancel()
        {
            if (State != ReplicationConnectionState.Streaming)
                throw new InvalidOperationException("Replication connection isn't in streaming state, can't cancel");

            Connection.Connector!.CancelRequest();
        }

        #endregion Replication commands

        private protected async Task<object[]> ReadSingleRow(string command)
        {
            var connector = Connection.Connector!;
            await connector.WriteQuery(command, true);
            await connector.Flush(true);

            var description = Expect<RowDescriptionMessage>(await connector.ReadMessage(true), connector);
            Expect<DataRowMessage>(await connector.ReadMessage(true), connector);
            var buf = connector.ReadBuffer;
            var results = new object[buf.ReadInt16()];
            for (var i = 0; i < results.Length; i++)
            {
                var len = buf.ReadInt32();
                if (len == -1)
                    continue;
                var str = buf.ReadString(len);
                var field = description.Fields[i];
                switch (field.PostgresType.Name)
                {
                case "text":
                    results[i] = str;
                    continue;
                case "integer":
                    if (!int.TryParse(str, out var num))
                    {
                        throw connector.Break(
                            new NpgsqlException($"Could not parse '{str}' as integer in field {field.Name}"));
                    }

                    results[i] = num;
                    continue;
                default:

                    throw connector.Break(new NpgsqlException(
                        $"Field {field.Name} has PostgreSQL type {field.PostgresType.Name} which isn't supported yet"));
                }
            }

            Expect<CommandCompleteMessage>(await connector.ReadMessage(true), connector);
            Expect<ReadyForQueryMessage>(await connector.ReadMessage(true), connector);
            return results;
        }

        private protected async Task SendFeedback(bool requestReply = false)
        {
            try
            {
                // Disable the timer while we are sending
                FeedbackTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

                var connector = Connection.Connector!;

                // This can only happen as a race condition if we're already disposed
                // in that state we can't use the connector any more so we back off
                if (connector == null)
                    return;

                await connector.WriteReplicationStatusUpdate(
                    LastReceivedLsn,
                    LastFlushedLsn,
                    LastAppliedLsn,
                    GetCurrentTimestamp(),
                    requestReply);
                await connector.Flush(true);
            }
            finally
            {
                // Restart the timer
                FeedbackTimer.Change(WalReceiverStatusInterval, Timeout.InfiniteTimeSpan);
                FeedbackSemaphore.Release();
            }
        }

        async void TimerSendFeedback(object? obj)
        {
            try
            {
                if (await FeedbackSemaphore.WaitAsync(TimeSpan.Zero))
                    await SendFeedback();
            }
            // The timer thread might race against Dispose() which means that FeedbackTimer might already
            // be disposed. We ignore that since we're in tear down mode anyways and timer feedback isn't considered
            // mandatory.
            catch (ObjectDisposedException) { }
        }

        #region SSL

        /// <summary>
        /// Selects the local Secure Sockets Layer (SSL) certificate used for authentication.
        /// </summary>
        /// <remarks>
        /// See <see href="https://msdn.microsoft.com/en-us/library/system.net.security.localcertificateselectioncallback(v=vs.110).aspx"/>
        /// </remarks>
        [PublicAPI]
        public ProvideClientCertificatesCallback? ProvideClientCertificatesCallback { get; set; }

        /// <summary>
        /// Verifies the remote Secure Sockets Layer (SSL) certificate used for authentication.
        /// Ignored if <see cref="NpgsqlConnectionStringBuilder.TrustServerCertificate"/> is set.
        /// </summary>
        /// <remarks>
        /// See <see href="https://msdn.microsoft.com/en-us/library/system.net.security.remotecertificatevalidationcallback(v=vs.110).aspx"/>
        /// </remarks>
        [PublicAPI]
        public RemoteCertificateValidationCallback? UserCertificateValidationCallback { get; set; }

        #endregion SSL

        #region Close / Dispose

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            await DisposeAsyncCore();

            Dispose(false);
            GC.SuppressFinalize(this);
        }

        private protected virtual async ValueTask DisposeAsyncCore()
        {
            if (Interlocked.Exchange(ref _disposing, 1) == 1)
                return;

            // If there's a running feedback, wait for it to finish, then grab FeedbackSemaphore and never release it
            // again.
            // We don't dispose FeedbackSemaphore though since SemaphoreSlim.Dispose() isn't thread safe and the current
            // implementation doesn't actually dispose anything unless you use the AvailableWaitHandle property.
            // ToDO: Think about a reasonable timeout and what to do in case of a timeout.
            // We definitely don't want to block in dispose forever.
            await FeedbackSemaphore.WaitAsync();

#if NET461 || NETSTANDARD2_0
            FeedbackTimer.Dispose();
#else
            await FeedbackTimer.DisposeAsync();
#endif
            if (Connection != null)
                await Connection.DisposeAsync();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (Interlocked.Exchange(ref _disposing, 1) == 1)
                    return;

                FeedbackSemaphore.Wait();

                FeedbackTimer.Dispose();
                Connection?.Dispose();
            }

            State = ReplicationConnectionState.Disposed;
        }

        #endregion Close / Dispose

        void CheckReady()
        {
            switch (State)
            {
            case ReplicationConnectionState.Closed:
                throw new InvalidOperationException("Connection is not open");
            case ReplicationConnectionState.Streaming:
                throw new InvalidOperationException("Connection is currently streaming, cancel before attempting a new operation");
            case ReplicationConnectionState.Disposed:
                throw new ObjectDisposedException(GetType().Name);
            }
            Connection.CheckReady();
        }

        long GetCurrentTimestamp() => (DateTime.Now.Ticks - 630822888000000000L) / 10;

        private protected enum ReplicationConnectionState
        {
            Closed,
            Idle,
            Streaming,
            Disposed
        }
    }
}
