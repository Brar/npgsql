using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Npgsql.Replication.Logical.TestDecoding
{
    /// <summary>
    /// Wraps a replication slot that uses the "test_decoding" logical decoding plugin which can be used to perform
    /// streaming replication using <see cref="NpgsqlTestDecodingData"/> messages.
    /// </summary>
    [PublicAPI]
    public sealed class NpgsqlTestDecodingReplicationSlot : NpgsqlLogicalReplicationSlot<NpgsqlTestDecodingData>
    {
        internal NpgsqlTestDecodingReplicationSlot(NpgsqlLogicalReplicationConnection connection, string slotName,
            LogSequenceNumber consistentPoint, string snapshotName) : base(connection, slotName,
            consistentPoint, snapshotName, "test_decoding")
        {
        }

        /// <summary>
        /// Instructs the server to start streaming WAL for logical replication, starting at WAL location
        /// <paramref name="walLocation"/>. The server can reply with an error, for example if the requested section of
        /// WAL has already been recycled.
        /// </summary>
        /// <param name="walLocation">The WAL location to begin streaming at.</param>
        /// <returns>An <see cref="IAsyncEnumerable{NpgsqlTestDecodingData}"/> streaming WAL entries in form of
        /// <see cref="NpgsqlTestDecodingData"/> instances.</returns>
        [PublicAPI]
        public override async Task<IAsyncEnumerable<NpgsqlTestDecodingData>> StartReplication(LogSequenceNumber? walLocation = null)
        {
            var stream = await StartReplicationStream(walLocation);

            return ParseMessages(stream);

            async IAsyncEnumerable<NpgsqlTestDecodingData> ParseMessages(IAsyncEnumerable<XLogData> xLogDataStream)
            {
                await foreach (var xlogData in xLogDataStream)
                {
                    yield return new NpgsqlTestDecodingData(xlogData.WalStart, xlogData.WalEnd, xlogData.ServerClock,
                        await GetString(xlogData.Data));
                }
            }
        }

        async Task<string> GetString(Stream xlogDataStream)
        {
            var memoryStream = new MemoryStream();
            await xlogDataStream.CopyToAsync(memoryStream);
            // ToDo: Plugins are probably useless if they don't know the encoding that's used. Expose it.
            return Connection.Connection.Connector!.TextEncoding.GetString(memoryStream.ToArray());
        }
    }
}
