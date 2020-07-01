using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.Logging;

#pragma warning disable 1591

namespace Npgsql.Replication
{
    /// <summary>
    ///
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
                        var sendTime = buf.ReadInt64();

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

        public async IAsyncEnumerable<OutputReplicationMessage> StartReplication(
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
                _bypassingStream = true;
                await foreach (var xLogData in await StartReplicationStream(slotName, walLocation, options))
                {
                    // Note that we bypass xLogData.Stream and access the connector's read buffer directly. This is
                    // an ugly hack, but allows us to use all the I/O methods and buffering that are already implemented.
                    await buf.EnsureAsync(1);
                    var messageCode = (BackendReplicationMessageCode)buf.ReadByte();
                    switch (messageCode)
                    {
                    case BackendReplicationMessageCode.Begin:
                    {
                        await buf.EnsureAsync(8 + 8 + 4);
                        yield return new BeginMessage
                        {
                            WalStart = xLogData.WalStart,
                            WalEnd = xLogData.WalEnd,
                            ServerClock = xLogData.ServerClock,

                            TransactionFinalLsn = buf.ReadUInt64(),
                            TransactionCommitTimestamp = buf.ReadUInt64(),
                            TransactionXid = buf.ReadUInt32()
                        };
                        continue;
                    }
                    case BackendReplicationMessageCode.Commit:
                    {
                        await buf.EnsureAsync(1 + 8 + 8 + 8);
                        yield return new CommitMessage
                        {
                            WalStart = xLogData.WalStart,
                            WalEnd = xLogData.WalEnd,
                            ServerClock = xLogData.ServerClock,

                            Flags = buf.ReadByte(),
                            CommitLsn = buf.ReadUInt64(),
                            TransactionEndLsn = buf.ReadUInt64(),
                            TransactionCommitTimestamp = buf.ReadUInt64()
                        };
                        continue;
                    }
                    case BackendReplicationMessageCode.Origin:
                        throw new NotImplementedException();
                    case BackendReplicationMessageCode.Relation:
                    {
                        await buf.EnsureAsync(4 + 1 + 1 + 1 + 2);
                        var x = new RelationMessage
                        {
                            WalStart = xLogData.WalStart,
                            WalEnd = xLogData.WalEnd,
                            ServerClock = xLogData.ServerClock,

                            RelationId = buf.ReadUInt32(),
                            Namespace = buf.ReadNullTerminatedString(),
                            RelationName = buf.ReadNullTerminatedString(),
                            RelationReplicaIdentitySetting = (char)buf.ReadByte()
                        };
                        var numColumns = buf.ReadUInt16();
                        for (var i = 0; i < numColumns; i++)
                        {
                            x.Columns.Add(
                                new RelationMessage.RelationColumn
                                {
                                    Flags = buf.ReadByte(),
                                    ColumnName = buf.ReadNullTerminatedString(),
                                    DataTypeId = buf.ReadUInt32(),
                                    TypeModifier = buf.ReadInt32()
                                });
                        }
                        yield return x;
                        continue;
                    }
                    case BackendReplicationMessageCode.Type:
                        throw new NotImplementedException();
                    case BackendReplicationMessageCode.Insert:
                    {
                        await buf.EnsureAsync(4 + 1 + 2);
                        var msg = new InsertMessage
                        {
                            WalStart = xLogData.WalStart,
                            WalEnd = xLogData.WalEnd,
                            ServerClock = xLogData.ServerClock,

                            RelationId = buf.ReadUInt32(),
                        };
                        var tupleDataType = (TupleType)buf.ReadByte();
                        Debug.Assert(tupleDataType == TupleType.NewTuple);

                        var numColumns = buf.ReadUInt16();
                        await AddTupleDataAsync(numColumns, buf, msg.NewRow);
                        yield return msg;
                        continue;
                    }
                    case BackendReplicationMessageCode.Update:
                    {
                        await buf.EnsureAsync(4 + 1 + 2);
                        var msg = new UpdateMessage
                        {
                            WalStart = xLogData.WalStart,
                            WalEnd = xLogData.WalEnd,
                            ServerClock = xLogData.ServerClock,

                            RelationId = buf.ReadUInt32(),
                        };
                        var tupleDataType = (TupleType)buf.ReadByte();
                        var numColumns = buf.ReadUInt16();
                        switch (tupleDataType)
                        {
                            case TupleType.Key:
                                msg.KeyRow = new List<TupleData>(numColumns);
                                await AddTupleDataAsync(numColumns, buf, msg.KeyRow);
                                break;
                            case TupleType.OldTuple:
                                msg.OldRow = new List<TupleData>(numColumns);
                                await AddTupleDataAsync(numColumns, buf, msg.OldRow);
                                break;
                            case TupleType.NewTuple:
                                await AddTupleDataAsync(numColumns, buf, msg.NewRow);
                                yield return msg;
                                continue;
                            default:
                                throw new NotSupportedException($"The tuple data type '{tupleDataType}' is not supported.");
                        }
                        await buf.EnsureAsync(1 + 2);
                        tupleDataType = (TupleType)buf.ReadByte();
                        Debug.Assert(tupleDataType == TupleType.NewTuple);
                        numColumns = buf.ReadUInt16();
                        await AddTupleDataAsync(numColumns, buf, msg.NewRow);
                        yield return msg;
                        continue;
                    }
                    case BackendReplicationMessageCode.Delete:
                    {
                        await buf.EnsureAsync(4 + 1 + 2);
                        var msg = new DeleteMessage
                        {
                            WalStart = xLogData.WalStart,
                            WalEnd = xLogData.WalEnd,
                            ServerClock = xLogData.ServerClock,

                            RelationId = buf.ReadUInt32(),
                        };
                        var tupleDataType = (TupleType)buf.ReadByte();
                        var numColumns = buf.ReadUInt16();
                        switch (tupleDataType)
                        {
                        case TupleType.Key:
                            msg.KeyRow = new List<TupleData>(numColumns);
                            await AddTupleDataAsync(numColumns, buf, msg.KeyRow);
                            break;
                        case TupleType.OldTuple:
                            msg.OldRow = new List<TupleData>(numColumns);
                            await AddTupleDataAsync(numColumns, buf, msg.OldRow);
                            break;
                        default:
                            throw new NotSupportedException($"The tuple data type '{tupleDataType}' is not supported.");
                        }
                        yield return msg;
                        continue;
                    }
                    case BackendReplicationMessageCode.Truncate:
                    {
                        await buf.EnsureAsync(4 + 1 + 4);
                        var numRels = buf.ReadUInt32();
                        var msg = new TruncateMessage()
                        {
                            WalStart = xLogData.WalStart,
                            WalEnd = xLogData.WalEnd,
                            ServerClock = xLogData.ServerClock,

                            Options = buf.ReadByte()
                        };

                        for (var i = 0; i < numRels; i++)
                            msg.RelationIds.Add(buf.ReadUInt32());

                        yield return msg;
                        continue;
                    }
                    default:
                        throw new ArgumentOutOfRangeException();
                    }

                    static async Task AddTupleDataAsync(ushort numColumns, NpgsqlReadBuffer buffer, List<TupleData> row)
                    {
                        for (var i = 0; i < numColumns; i++)
                        {
                            await buffer.EnsureAsync(1);
                            var submessageType = (char)buffer.ReadByte();
                            switch (submessageType)
                            {
                            case 'n':
                                row.Add(
                                    new TupleData
                                    {
                                        Type = TupleDataType.Null
                                    });
                                break;
                            case 'u':
                                row.Add(
                                    new TupleData
                                    {
                                        Type = TupleDataType.UnchangedToastedValue
                                    });
                                break;
                            case 't':
                                await buffer.EnsureAsync(4);
                                var len = buffer.ReadInt32();
                                await buffer.EnsureAsync(len);
                                row.Add(
                                    new TupleData
                                    {
                                        Type = TupleDataType.TextValue,
                                        Value = buffer.ReadString(len)
                                    });
                                break;
                            default:
                                throw new NotSupportedException($"The TupleData submessage type '{submessageType}' is not supported.");
                            }
                        }
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

        #region Support types

        // TODO: Inner type?
        /// <summary>
        /// Decides what to do with the snapshot created during logical slot initialization.
        /// </summary>
        [PublicAPI]
        public enum SlotSnapshotInitMode
        {
            /// <summary>
            /// Export the snapshot for use in other sessions. This is the default.
            /// This option can't be used inside a transaction.
            /// </summary>
            Export,

            /// <summary>
            /// Use the snapshot for the current transaction executing the command.
            /// This option must be used in a transaction, and CREATE_REPLICATION_SLOT must be the first command run
            /// in that transaction.
            /// </summary>
            Use,

            /// <summary>
            /// Just use the snapshot for logical decoding as normal but don't do anything else with it.
            /// </summary>
            NoExport
        }

        /// <summary>
        /// Contains information about a newly-created logical replication slot.
        /// </summary>
        [PublicAPI]
        public readonly struct NpgsqlLogicalReplicationSlotInfo
        {
            internal NpgsqlLogicalReplicationSlotInfo(
                string slotName,
                string consistentPoint,
                string snapshotName,
                string outputPlugin)
            {
                SlotName = slotName;
                ConsistentPoint = consistentPoint;
                SnapshotName = snapshotName;
                OutputPlugin = outputPlugin;
            }

            /// <summary>
            /// The name of the newly-created replication slot.
            /// </summary>
            public string SlotName { get; }

            /// <summary>
            /// The WAL location at which the slot became consistent.
            /// This is the earliest location from which streaming can start on this replication slot.
            /// </summary>
            public string ConsistentPoint { get; }

            /// <summary>
            /// The identifier of the snapshot exported by the command.
            /// The snapshot is valid until a new command is executed on this connection or the replication connection is closed.
            /// </summary>
            public string SnapshotName { get; }

            /// <summary>
            /// The name of the output plugin used by the newly-created replication slot.
            /// </summary>
            public string OutputPlugin { get; }
        }

        /// <summary>
        /// A message representing a section of the WAL data stream.
        /// </summary>
        [PublicAPI]
        public readonly struct XLogData
        {
            internal XLogData(
                ulong walStart,
                ulong walEnd,
                long serverClock,
                Stream data)
            {
                WalStart = walStart;
                WalEnd = walEnd;
                ServerClock = serverClock;
                Data = data;
            }

            /// <summary>
            /// The starting point of the WAL data in this message.
            /// </summary>
            public ulong WalStart { get; }

            /// <summary>
            /// The current end of WAL on the server.
            /// </summary>
            public ulong WalEnd { get; }

            /// <summary>
            /// The server's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
            /// </summary>
            public long ServerClock { get; }

            /// <summary>
            /// A section of the WAL data stream.
            /// </summary>
            /// <remarks>
            /// A single WAL record is never split across two XLogData messages.
            /// When a WAL record crosses a WAL page boundary, and is therefore already split using continuation records,
            /// it can be split at the page boundary. In other words, the first main WAL record and its continuation
            /// records can be sent in different XLogData messages.
            /// </remarks>
            public Stream Data { get; }
        }

        #endregion Support types
    }
}
