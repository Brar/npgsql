using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Npgsql.Replication.Physical
{
    /// <summary>
    /// Wraps a replication slot that uses physical replication.
    /// </summary>
    [PublicAPI]
    public class NpgsqlPhysicalReplicationSlot : NpgsqlReplicationSlot<XLogData>
    {
        internal NpgsqlPhysicalReplicationSlot(NpgsqlReplicationConnection connection, string slotName,
            LogSequenceNumber consistentPoint) : base(connection, slotName, consistentPoint)
        {
        }

        /// <summary>
        /// Instructs the server to start streaming WAL for logical replication, starting at WAL location
        /// <paramref name="walLocation"/>. The server can reply with an error, for example if the requested section of
        /// WAL has already been recycled.
        /// </summary>
        /// <remarks>
        /// If the client requests a timeline that's not the latest but is part of the history of the server, the server
        /// will stream all the WAL on that timeline starting from the requested start point up to the point where the
        /// server switched to another timeline. If the client requests streaming at exactly the end of an old timeline,
        /// the server responds immediately with CommandComplete without entering COPY mode.
        /// </remarks>
        /// <param name="walLocation">The WAL location to begin streaming at.</param>
        /// <returns>An <see cref="IAsyncEnumerable{XLogData}"/> streaming WAL entries in form of
        /// <see cref="XLogData"/> values.</returns>
        [PublicAPI]
        public override Task<IAsyncEnumerable<XLogData>> StartReplication(LogSequenceNumber? walLocation = null)
            => StartReplicationInternal(null, walLocation);

        /// <summary>
        /// Instructs the server to start streaming WAL for logical replication, starting at WAL location
        /// <paramref name="walLocation"/>. The server can reply with an error, for example if the requested section of
        /// WAL has already been recycled.
        /// </summary>
        /// <remarks>
        /// If the client requests a timeline that's not the latest but is part of the history of the server, the server
        /// will stream all the WAL on that timeline starting from the requested start point up to the point where the
        /// server switched to another timeline. If the client requests streaming at exactly the end of an old timeline,
        /// the server responds immediately with CommandComplete without entering COPY mode.
        /// </remarks>
        /// <param name="timeline">Streaming starts on timeline tli.</param>
        /// <param name="walLocation">The WAL location to begin streaming at.</param>
        /// <returns>An <see cref="IAsyncEnumerable{XLogData}"/> streaming WAL entries in form of
        /// <see cref="XLogData"/> values.</returns>
        [PublicAPI]
        public Task<IAsyncEnumerable<XLogData>> StartReplication(string timeline, LogSequenceNumber? walLocation = null)
            => StartReplicationInternal(timeline, walLocation);

        // ToDo: Investigate if there's a better representation for timeline than string.
        async Task<IAsyncEnumerable<XLogData>> StartReplicationInternal(string? timeline,
            LogSequenceNumber? walLocation = null)
        {
            var sb = new StringBuilder("START_REPLICATION SLOT ")
                .Append(SlotName)
                .Append(" PHYSICAL ")
                .Append(walLocation?.ToString() ?? ConsistentPoint.ToString());

            if (timeline != null)
                sb.Append(' ').Append(timeline);

            var stream = await Connection.StartReplication(sb.ToString(), false);
            return StartStreaming(stream);

            async IAsyncEnumerable<XLogData> StartStreaming(IAsyncEnumerable<XLogData> xlogDataStream)
            {
                await foreach (var xLogData in xlogDataStream)
                    yield return xLogData;
            }
        }
    }
}
