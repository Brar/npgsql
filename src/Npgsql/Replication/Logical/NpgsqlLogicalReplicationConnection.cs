using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.Logging;
using Npgsql.Replication.Logical.Protocol;
using Npgsql.TypeHandlers.DateTimeHandlers;

namespace Npgsql.Replication.Logical
{
    /// <summary>
    /// Represents a logical replication connection to a PostgreSQL server
    /// </summary>
    public sealed class NpgsqlLogicalReplicationConnection : NpgsqlReplicationConnection
    {
        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(NpgsqlLogicalReplicationConnection));
        bool _bypassingStream = false;

        /// <summary>
        /// Initializes a new instance of <see cref="NpgsqlLogicalReplicationConnection"/>.
        /// </summary>
        public NpgsqlLogicalReplicationConnection() {}

        /// <summary>
        /// Initializes a new instance of <see cref="NpgsqlLogicalReplicationConnection"/>.
        /// </summary>
        public NpgsqlLogicalReplicationConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        #region Open

        /// <summary>
        /// Opens a database replication connection with the property settings specified by the
        /// <see cref="NpgsqlReplicationConnection.ConnectionString">ConnectionString</see>.
        /// </summary>
        [PublicAPI]
        public Task OpenAsync(CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
            {
                return OpenAsync(new NpgsqlConnectionStringBuilder(ConnectionString)
                {
                    ReplicationMode = ReplicationMode.Logical
                }, cancellationToken);
            }
        }

        #endregion Open

        #region Replication commands

        static readonly Version V10_0 = new Version(10, 0);

        /// <summary>
        /// Create a logical replication slot.
        /// </summary>
        /// <param name="slotName">
        /// The name of the slot to create. Must be a valid replication slot name
        /// (see <a href="https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">Section 26.2.6.1</a>).
        /// </param>
        /// <param name="isTemporary">
        /// Specify that this replication slot is a temporary one.
        /// Temporary slots are not saved to disk and are automatically dropped on error or when the session has finished.
        /// </param>
        /// <param name="outputPlugin">
        /// The name of the output plugin used for logical decoding
        /// (see <a href="https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html">Section 49.6</a>).
        /// </param>
        /// <param name="slotSnapshotInitMode">
        /// Decides what to do with the snapshot created during logical slot initialization.
        /// </param>
        /// <returns>
        /// An <see cref="NpgsqlLogicalReplicationSlotInfo"/> providing information on the newly-created slot.
        /// </returns>
        /// <remarks>
        /// See https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS.
        /// </remarks>
        [PublicAPI]
        public async Task<NpgsqlLogicalReplicationSlotInfo> CreateReplicationSlot(
            string slotName,
            string outputPlugin = "pgoutput",
            bool isTemporary = false,
            SlotSnapshotInitMode slotSnapshotInitMode = SlotSnapshotInitMode.Export)
        {
            var sb = new StringBuilder("CREATE_REPLICATION_SLOT ").Append(slotName);
            if (isTemporary)
                sb.Append(" TEMPORARY");
            sb.Append(" LOGICAL ").Append(outputPlugin);

            sb.Append(slotSnapshotInitMode switch
            {
                // EXPORT_SNAPSHOT is the default.
                // We don't set it explicitly so that older backends can digest the query too.
                SlotSnapshotInitMode.Export => string.Empty,
                SlotSnapshotInitMode.Use => " USE_SNAPSHOT",
                SlotSnapshotInitMode.NoExport => " NOEXPORT_SNAPSHOT",
                _ => throw new ArgumentOutOfRangeException(nameof(slotSnapshotInitMode),
                    slotSnapshotInitMode,
                    $"Unexpected value {slotSnapshotInitMode} for argument {nameof(slotSnapshotInitMode)}.")
            });
            try
            {
                var results = await ReadSingleRow(sb.ToString());
                return new NpgsqlLogicalReplicationSlotInfo(
                    (string)results[0],
                    (string)results[1],
                    (string)results[2],
                    (string)results[3]
                );
            }
            catch (PostgresException e)
            {
                if (Connection.PostgreSqlVersion < V10_0 && e.SqlState == "42601" /* syntax_error */)
                {
                    if (isTemporary)
                        throw new ArgumentException($"Temporary replication slots were introduced in PostgreSQL 10. Using PostgreSQL version {Connection.PostgreSqlVersion.ToString(3)} you have to leave the {nameof(isTemporary)} argument as false.", nameof(isTemporary), e);
                    if (slotSnapshotInitMode != SlotSnapshotInitMode.Export)
                        throw new ArgumentException($"The USE_SNAPSHOT and NOEXPORT_SNAPSHOT syntax was introduced in PostgreSQL 10. Using PostgreSQL version {Connection.PostgreSqlVersion.ToString(3)} you have to leave the {nameof(slotSnapshotInitMode)} argument as {nameof(SlotSnapshotInitMode)}.{nameof(SlotSnapshotInitMode.NoExport)}.", nameof(slotSnapshotInitMode), e);
                }
                throw;
            }
        }

        /// <summary>
        /// Instructs server to start streaming WAL for logical replication, starting at WAL location <paramref name="walLocation"/>.
        /// The server can reply with an error, for example if the requested section of WAL has already been recycled.
        /// </summary>
        /// <param name="walLocation">
        /// The WAL location from which to start streaming, in the format XXX/XXX.
        /// </param>
        /// <param name="slotName">
        /// If a slot's name is provided, it will be updated as replication progresses so that the server knows which
        /// WAL segments, and if hot_standby_feedback is on which transactions, are still needed by the standby.
        /// </param>
        /// <param name="options">
        /// Options to be passed to the slot's logical decoding plugin.
        /// </param>
        [PublicAPI]
        public async Task<IAsyncEnumerable<XLogData>> StartReplicationStream(string slotName, string? walLocation = null, Dictionary<string, string>? options = null)
        {
            var sb = new StringBuilder("START_REPLICATION SLOT ")
                .Append(slotName)
                .Append(" LOGICAL ")
                .Append(walLocation);

            if (options != null)
            {
                sb
                    .Append(" (")
                    .Append(string.Join(", ", options
                        .Select(kv => kv.Value is null ? $"\"{kv.Key}\"" : $"\"{kv.Key}\" '{kv.Value}'")))
                    .Append(")");
            }

            var connector = Connection.Connector!;
            await connector.WriteQuery(sb.ToString(), true);
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
                        else if (columnStream.Position < columnStream.Length && !_bypassingStream)
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

        #endregion Replication commands

        #region PG output plugin

        /// <summary>
        /// Starts streaming replication using the Logical Replication Protocol (using the pretty much undocumented
        /// pgoutput plugin).
        /// </summary>
        /// <remarks>
        /// See https://www.postgresql.org/docs/current/logical-replication.html to get an idea how to set up logical
        /// replication via Logical Replication Protocol.
        /// Keep in mind that in this case Npgsql represents the subscriber (instead of a second PostgreSQL server) and
        /// that you only have to set up the publisher side (publications via CREATE PUBLICATION;
        /// https://www.postgresql.org/docs/current/sql-createpublication.html).
        /// </remarks>
        /// <param name="slotName">
        /// The name of the slot to stream changes from.
        /// It must correspond to an existing logical replication slot.</param>
        /// <param name="walLocation">The WAL location to begin streaming at.</param>
        /// <param name="publicationNames">Names of the publications to subscribe to.</param>
        /// <returns>An <see cref="IAsyncEnumerable{LogicalReplicationProtocolMessage}" />that can be used to enumerate
        /// (stream) instances of <see cref="LogicalReplicationProtocolMessage"/>.</returns>
        /// <exception cref="NotSupportedException">
        /// This happens in cases where an unsupported backend message is
        /// received from the server. This is probably caused by a protocol mismatch where the server uses a newer
        /// replication protocol than Npgsql.
        /// </exception>
        public async IAsyncEnumerable<LogicalReplicationProtocolMessage> StartReplication(
            string slotName,
            string? walLocation, // TODO: Should be defaultable, maybe fluent API, maybe not
            params string[] publicationNames)  // TODO: Does the user need to specify at least one? If so, possibly force at least one publication name via a separate param
        {
            var buf = Connection.Connector!.ReadBuffer;

            var options = new Dictionary<string, string>
            {
                { "proto_version", "1" },
                { "publication_names", string.Join(",", publicationNames.Select(pn => $"\"{pn}\"")) }
            };
            try
            {
                // Note that we bypass xLogData.Stream and access the connector's read buffer directly.
                // This allows us to use all the I/O methods and buffering that are already implemented.
                _bypassingStream = true;
                await foreach (var xLogData in await StartReplicationStream(slotName, walLocation, options))
                {
                    await buf.EnsureAsync(1);
                    var messageCode = (BackendReplicationMessageCode)buf.ReadByte();
                    switch (messageCode)
                    {
                    case BackendReplicationMessageCode.Begin:
                    {
                        await buf.EnsureAsync(20);
                        yield return new BeginMessage(
                            xLogData.WalStart,
                            xLogData.WalEnd,
                            xLogData.ServerClock,
                            buf.ReadUInt64(),
                            TimestampHandler.Int64ToNpgsqlDateTime(buf.ReadInt64()).ToDateTime(),
                            buf.ReadUInt32()
                        );
                        continue;
                    }
                    case BackendReplicationMessageCode.Commit:
                    {
                        await buf.EnsureAsync(25);
                        yield return new CommitMessage(
                            xLogData.WalStart,
                            xLogData.WalEnd,
                            xLogData.ServerClock,
                            buf.ReadByte(),
                            buf.ReadUInt64(),
                            buf.ReadUInt64(),
                            TimestampHandler.Int64ToNpgsqlDateTime(buf.ReadInt64()).ToDateTime()
                        );
                        continue;
                    }
                    case BackendReplicationMessageCode.Origin:
                    {
                        await buf.EnsureAsync(9);
                        yield return new OriginMessage(
                            xLogData.WalStart,
                            xLogData.WalEnd,
                            xLogData.ServerClock,
                            buf.ReadUInt64(),
                            await buf.ReadNullTerminatedStringAsync());
                        continue;
                    }
                    case BackendReplicationMessageCode.Relation:
                    {
                        await buf.EnsureAsync(6);
                        var relationId = buf.ReadUInt32();
                        var ns = await buf.ReadNullTerminatedStringAsync();
                        var relationName = await buf.ReadNullTerminatedStringAsync();
                        await buf.EnsureAsync(3);
                        var relationReplicaIdentitySetting = (char)buf.ReadByte();
                        var numColumns = buf.ReadUInt16();
                        var columns = new RelationMessageColumnList(numColumns);
                        for (var i = 0; i < numColumns; i++)
                        {
                            await buf.EnsureAsync(2);
                            var flags = buf.ReadByte();
                            var columnName = await buf.ReadNullTerminatedStringAsync();
                            await buf.EnsureAsync(8);
                            var dateTypeId = buf.ReadUInt32();
                            var typeModifier = buf.ReadInt32();
                            columns.InternalList.Add(new RelationMessageColumn(flags, columnName, dateTypeId,
                                typeModifier));
                        }

                        yield return new RelationMessage(
                            xLogData.WalStart,
                            xLogData.WalEnd,
                            xLogData.ServerClock,
                            relationId,
                            ns,
                            relationName,
                            relationReplicaIdentitySetting,
                            columns
                        );

                        continue;
                    }
                    case BackendReplicationMessageCode.Type:
                    {
                        await buf.EnsureAsync(5);
                        var typeId = buf.ReadUInt32();
                        var ns = await buf.ReadNullTerminatedStringAsync();
                        var name = await buf.ReadNullTerminatedStringAsync();
                        yield return new TypeMessage(xLogData.WalStart, xLogData.WalEnd, xLogData.ServerClock, typeId,
                            ns, name);

                        continue;
                    }
                    case BackendReplicationMessageCode.Insert:
                    {
                        await buf.EnsureAsync(7);
                        var relationId = buf.ReadUInt32();
                        var tupleDataType = (TupleType)buf.ReadByte();
                        Debug.Assert(tupleDataType == TupleType.NewTuple);
                        var numColumns = buf.ReadUInt16();
                        var newRow = await ReadTupleDataAsync(numColumns);
                        yield return new InsertMessage(xLogData.WalStart, xLogData.WalEnd, xLogData.ServerClock,
                            relationId, newRow);

                        continue;
                    }
                    case BackendReplicationMessageCode.Update:
                    {
                        await buf.EnsureAsync(7);
                        var relationId = buf.ReadUInt32();
                        var tupleType = (TupleType)buf.ReadByte();
                        var numColumns = buf.ReadUInt16();
                        switch (tupleType)
                        {
                            case TupleType.Key:
                                var keyRow = await ReadTupleDataAsync(numColumns);
                                await buf.EnsureAsync(3);
                                tupleType = (TupleType)buf.ReadByte();
                                Debug.Assert(tupleType == TupleType.NewTuple);
                                numColumns = buf.ReadUInt16();
                                var newRow = await ReadTupleDataAsync(numColumns);
                                yield return new IndexUpdateMessage(xLogData.WalStart, xLogData.WalEnd,
                                    xLogData.ServerClock, relationId, newRow, keyRow);
                                continue;
                            case TupleType.OldTuple:
                                var oldRow = await ReadTupleDataAsync(numColumns);
                                await buf.EnsureAsync(3);
                                tupleType = (TupleType)buf.ReadByte();
                                Debug.Assert(tupleType == TupleType.NewTuple);
                                numColumns = buf.ReadUInt16();
                                newRow = await ReadTupleDataAsync(numColumns);
                                yield return new FullUpdateMessage(xLogData.WalStart, xLogData.WalEnd,
                                    xLogData.ServerClock, relationId, newRow, oldRow);
                                continue;
                            case TupleType.NewTuple:
                                newRow = await ReadTupleDataAsync(numColumns);
                                yield return new UpdateMessage(xLogData.WalStart, xLogData.WalEnd,
                                    xLogData.ServerClock, relationId, newRow);
                                continue;
                            default:
                                throw new NotSupportedException($"The tuple type '{tupleType}' is not supported.");
                        }
                    }
                    case BackendReplicationMessageCode.Delete:
                    {
                        await buf.EnsureAsync(7);
                        var relationId = buf.ReadUInt32();
                        var tupleDataType = (TupleType)buf.ReadByte();
                        var numColumns = buf.ReadUInt16();
                        switch (tupleDataType)
                        {
                        case TupleType.Key:
                            yield return new KeyDeleteMessage(xLogData.WalStart, xLogData.WalEnd, xLogData.ServerClock,
                                relationId, await ReadTupleDataAsync(numColumns));
                            continue;
                        case TupleType.OldTuple:
                            yield return new FullDeleteMessage(xLogData.WalStart, xLogData.WalEnd, xLogData.ServerClock,
                                relationId, await ReadTupleDataAsync(numColumns));
                            continue;
                        default:
                            throw new NotSupportedException($"The tuple type '{tupleDataType}' is not supported.");
                        }
                    }
                    case BackendReplicationMessageCode.Truncate:
                    {
                        await buf.EnsureAsync(9);
                        // Don't dare to truncate more than 2147483647 tables at once!
                        var numRels = checked((int)buf.ReadUInt32());
                        var truncateOptions = (TruncateOptions)buf.ReadByte();
                        var relationIds = new RelationIdList(numRels);
                        await buf.EnsureAsync(checked(numRels * 4));

                        for (var i = 0; i < numRels; i++)
                            relationIds.InternalList.Add(buf.ReadUInt32());

                        yield return new TruncateMessage(xLogData.WalStart, xLogData.WalEnd, xLogData.ServerClock,
                            truncateOptions, relationIds);
                        continue;
                    }
                    default:
                        throw new NotSupportedException($"Invalid message code {messageCode} in Logical Replication Protocol.");
                    }

                    async Task<TupleDataList> ReadTupleDataAsync(ushort numberOfColumns)
                    {
                        var ret = new TupleDataList(numberOfColumns);
                        for (var i = 0; i < numberOfColumns; i++)
                        {
                            await buf.EnsureAsync(1);
                            var subMessageKind = (TupleDataKind)buf.ReadByte();
                            switch (subMessageKind)
                            {
                            case TupleDataKind.Null:
                            case TupleDataKind.UnchangedToastedValue:
                                ret.InternalList.Add(new TupleData(subMessageKind));
                                break;
                            case TupleDataKind.TextValue:
                                await buf.EnsureAsync(4);
                                var len = buf.ReadInt32();
                                await buf.EnsureAsync(len);
                                ret.InternalList.Add(new TupleData(buf.ReadString(len)));
                                break;
                            default:
                                throw new NotSupportedException($"The tuple data kind '{subMessageKind}' is not supported.");
                            }
                        }

                        return ret;
                    }
                }
            }
            finally
            {
                _bypassingStream = false;
            }

            //yield return null!;
            throw new NotImplementedException();
        }

        #endregion PG output plugin
    }
}
