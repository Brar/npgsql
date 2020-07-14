using System.Collections.Generic;
using System.Threading.Tasks;

namespace Npgsql.Replication.Logical.Raw
{
    /// <summary>
    /// Wraps a replication slot that can use an arbitrary logical decoding plugin. The streaming replication will
    /// use raw <see cref="XLogData"/> messages without any further processing of the data stream.
    /// </summary>
    public class NpgsqlRawLogicalReplicationSlot : NpgsqlLogicalReplicationSlot<XLogData>
    {
        internal NpgsqlRawLogicalReplicationSlot(NpgsqlLogicalReplicationConnection connection, string slotName,
            LogSequenceNumber consistentPoint, string snapshotName, string outputPlugin) : base(connection, slotName,
            consistentPoint, snapshotName, outputPlugin)
        {
        }

        /// <inheritdoc cref="NpgsqlLogicalReplicationSlot{XlogData}.StartReplicationStream(LogSequenceNumber?, Dictionary{string, string?})" />
        public Task<IAsyncEnumerable<XLogData>> StartReplication(Dictionary<string, string?>? options, LogSequenceNumber? walLocation = null)
            => StartReplicationStream(walLocation, options);

        /// <inheritdoc />
        public override Task<IAsyncEnumerable<XLogData>> StartReplication(LogSequenceNumber? walLocation = null)
            => StartReplicationStream(walLocation);
    }
}
