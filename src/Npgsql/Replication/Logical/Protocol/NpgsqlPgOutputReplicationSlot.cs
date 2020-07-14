using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.TypeHandlers.DateTimeHandlers;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Wraps a replication slot that uses the "pgoutput" logical decoding plugin which can be used to perform streaming
    /// replication using <see cref="LogicalReplicationProtocolMessage"/> instances.
    /// </summary>
    [PublicAPI]
    public sealed class NpgsqlPgOutputReplicationSlot : NpgsqlLogicalReplicationSlot<LogicalReplicationProtocolMessage>
    {
        internal NpgsqlPgOutputReplicationSlot(NpgsqlLogicalReplicationConnection connection, string slotName,
            LogSequenceNumber consistentPoint, string snapshotName)
            : base(connection, slotName, consistentPoint, snapshotName, "pgoutput")
        {
        }

        /// <summary>
        /// Instructs the server to start streaming WAL for logical replication, starting at WAL location
        /// <paramref name="walLocation"/>. The server can reply with an error, for example if the requested section of
        /// WAL has already been recycled.
        /// </summary>
        /// <param name="publicationName">The name of the publication to stream. See
        /// https://www.postgresql.org/docs/current/logical-replication-publication.html</param>
        /// <param name="walLocation">The WAL location to begin streaming at.</param>
        /// <returns>An <see cref="IAsyncEnumerable{LogicalReplicationProtocolMessage}"/> streaming WAL entries in form
        /// of <see cref="LogicalReplicationProtocolMessage"/> instances.</returns>
        /// <returns></returns>
        public Task<IAsyncEnumerable<LogicalReplicationProtocolMessage>> StartReplication(string publicationName,
            LogSequenceNumber? walLocation = null)
            => StartReplication(new[] { publicationName }, walLocation);

        /// <summary>
        /// Instructs the server to start streaming WAL for logical replication, starting at WAL location
        /// <paramref name="walLocation"/>. The server can reply with an error, for example if the requested section of
        /// WAL has already been recycled.
        /// </summary>
        /// <param name="publicationNames">The names of the publications to stream. See
        /// https://www.postgresql.org/docs/current/logical-replication-publication.html</param>
        /// <param name="walLocation">The WAL location to begin streaming at.</param>
        /// <returns>An <see cref="IAsyncEnumerable{LogicalReplicationProtocolMessage}"/> streaming WAL entries in form
        /// of <see cref="LogicalReplicationProtocolMessage"/> instances.</returns>
        [PublicAPI]
        public async Task<IAsyncEnumerable<LogicalReplicationProtocolMessage>> StartReplication(
            IEnumerable<string> publicationNames, LogSequenceNumber? walLocation = null)
        {
            var publications = FormatPublicationNames(publicationNames) ??
                               throw new ArgumentException("You have to specify at least one publication name",
                                   nameof(publicationNames));
            var buf = Connection.Connection.Connector!.ReadBuffer;

            var options = new Dictionary<string, string?>
            {
                { "proto_version", "1" },
                { "publication_names", publications }

            };

            var stream = await StartReplicationStream(true, walLocation, options);

            return ParseMessages(stream);

            async IAsyncEnumerable<LogicalReplicationProtocolMessage> ParseMessages(IAsyncEnumerable<XLogData> xLogDataStream)
            {
                // Note that we bypass xLogData.Stream and access the connector's read buffer directly.
                // This allows us to use all the I/O methods and buffering that are already implemented.
                await foreach (var xLogData in xLogDataStream)
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
                        throw new NotSupportedException(
                            $"Invalid message code {messageCode} in Logical Replication Protocol.");
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
                                throw new NotSupportedException(
                                    $"The tuple data kind '{subMessageKind}' is not supported.");
                            }
                        }

                        return ret;
                    }
                }

                //yield return null!;
                throw new NotImplementedException();
            }

        }

        /// <inheritdoc />
        /// <remarks>
        /// This overload is not supported by <see cref="NpgsqlPgOutputReplicationSlot"/>. You have to specify at least
        /// one publication name.
        /// </remarks>
        public override Task<IAsyncEnumerable<LogicalReplicationProtocolMessage>> StartReplication(
            LogSequenceNumber? walLocation = null)
            => throw new NotSupportedException(
                $"This overload is not supported by {nameof(NpgsqlPgOutputReplicationSlot)}. You have to specify at least one publication name.");

        static string? FormatPublicationNames(IEnumerable<string> publicationNames)
        {
            using var enumerator = publicationNames.GetEnumerator();
            if (!enumerator.MoveNext()) return null;
            var sb = new StringBuilder();
            sb.Append('"').Append(enumerator.Current).Append('"');
            while (enumerator.MoveNext()) sb.Append(",\"").Append(enumerator.Current).Append('"');
            return sb.ToString();
        }
    }
}
