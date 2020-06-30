using System;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.Logging;
using static Npgsql.Util.Statics;

namespace Npgsql.Replication
{
    /// <summary>
    ///
    /// </summary>
    public abstract class NpgsqlReplicationConnection : IDisposable, IAsyncDisposable
    {
        #region Fields

        private protected NpgsqlConnection Connection = default!;
        private protected ReplicationConnectionState State { get; set; }
        private protected readonly Timer FeedbackTimer;
        int _timerFence;
        TimeSpan _walReceiverStatusInterval = TimeSpan.FromSeconds(10d);
        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(NpgsqlReplicationConnection));

        #endregion Fields

        /// <summary>
        /// Initializes a new instance of <see cref="NpgsqlReplicationConnection"/>.
        /// </summary>
        protected NpgsqlReplicationConnection()
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
        public TimeSpan WalReceiverStatusInterval
        {
            get => _walReceiverStatusInterval;
            set
            {
                _walReceiverStatusInterval = value;
                FeedbackTimer.Change(TimeSpan.Zero, value);
            }
        }

        /// <summary>
        /// Time that receiver waits for communication from master.
        /// Timeout.<see cref="Timeout.InfiniteTimeSpan"/> disables the timeout.
        /// </summary>
        public TimeSpan WalReceiverTimeout { get; set; } = TimeSpan.FromSeconds(60d);

        #endregion Properties

        /// <summary>
        ///
        /// </summary>
        protected async Task OpenAsync(NpgsqlConnectionStringBuilder settings, CancellationToken cancellationToken)
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

            if (force)
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
        /// Requests the server to send the current setting of a run-time parameter.
        /// This is similar to the SQL command SHOW.
        /// </summary>
        [PublicAPI]
        public async Task<string> Show(string parameterName)
            => (string)(await ReadSingleRow("SHOW " + parameterName))[0];

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
                // If we come from TimerSendFeedback the _timerFence is already up and we leave the timer alone.
                // If we are a forced SendFeedback and the _timerFence is down we set it and reset the timer.
                if (Interlocked.CompareExchange(ref _timerFence, 1, 0) == 0)
                    FeedbackTimer.Change(_walReceiverStatusInterval, _walReceiverStatusInterval);

                var connector = Connection.Connector!;

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
                _timerFence = 0;
            }
        }

        async void TimerSendFeedback(object? obj)
        {
            if (Interlocked.CompareExchange(ref _timerFence, 1, 0) == 1)
                return;

            // This can only happen as a race condition if we're already disposed
            // We don't care about the fence anymore at this point.
            if (Connection == null)
                return;

            await SendFeedback();
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
            if (Connection != null)
            {
                await Connection.DisposeAsync();
                Connection = null!;
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private protected virtual void Dispose(bool disposing)
        {
            if (State == ReplicationConnectionState.Disposed)
                return;

            if (disposing)
            {
                Connection?.Dispose();
                Connection = null!;
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

    #region Support types

    /// <summary>
    /// Contains server identification information returned from <see cref="NpgsqlReplicationConnection.IdentifySystem"/>.
    /// </summary>
    [PublicAPI]
    public readonly struct NpgsqlReplicationIdentificationInfo
    {
        internal NpgsqlReplicationIdentificationInfo(
            string systemId,
            int timeline,
            string xLogPos,
            string dbName)
        {
            SystemId = systemId;
            Timeline = timeline;
            XLogPos = xLogPos;
            DbName = dbName;
        }

        /// <summary>
        /// The unique system identifier identifying the cluster.
        /// This can be used to check that the base backup used to initialize the standby came from the same cluster.
        /// </summary>
        public string SystemId { get; }

        /// <summary>
        /// Current timeline ID. Also useful to check that the standby is consistent with the master.
        /// </summary>
        public int Timeline { get; }

        /// <summary>
        /// Current WAL flush location. Useful to get a known location in the write-ahead log where streaming can start.
        /// </summary>
        public string XLogPos { get; }

        /// <summary>
        /// Database connected to or null.
        /// </summary>
        public string DbName { get; }
    }

    /// <summary>
    /// Contains the timeline history file for a timeline.
    /// </summary>
    public readonly struct NpgsqlTimelineHistoryFile
    {
        internal NpgsqlTimelineHistoryFile(string filename, string content)
        {
            Filename = filename;
            Content = content;
        }

        /// <summary>
        /// File name of the timeline history file, e.g., 00000002.history.
        /// </summary>
        public string Filename { get; }

        /// <summary>
        /// Contents of the timeline history file.
        /// </summary>
        public string Content { get; }
    }

    #endregion Support types
}
