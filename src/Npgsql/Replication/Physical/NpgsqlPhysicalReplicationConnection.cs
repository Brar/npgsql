using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.Logging;
#pragma warning disable 1591

namespace Npgsql.Replication.Physical
{
    public sealed class NpgsqlPhysicalReplicationConnection : NpgsqlReplicationConnection
    {
        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(NpgsqlPhysicalReplicationConnection));

        #region Open

        /// <summary>
        /// Opens a database replication connection with the property settings specified by the
        /// <see cref="NpgsqlReplicationConnection.ConnectionString">ConnectionString</see>.
        /// </summary>
        [PublicAPI]
        public override Task OpenAsync(CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
            {
                return OpenAsync(new NpgsqlConnectionStringBuilder(ConnectionString)
                {
                    ReplicationMode = ReplicationMode.Physical
                }, cancellationToken);
            }
        }

        #endregion Open

        #region Replication commands

        /// <summary>
        /// Creates a <see cref="NpgsqlPhysicalReplicationSlot"/> that wraps a PostgreSQL physical replication slot and
        /// can be used to start physical streaming replication
        /// </summary>
        /// <param name="slotName">
        /// The name of the slot to create. Must be a valid replication slot name
        /// (see <a href="https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">Section 26.2.6.1</a>).
        /// </param>
        /// <param name="temporary"><see langword="true"/> if this replication slot shall be temporary one; otherwise
        /// <see langword="false"/>. Temporary slots are not saved to disk and are automatically dropped on error or
        /// when the session has finished.</param>
        /// <param name="reserveWal">
        /// If this is set to <see langword="true"/> this physical replication slot reserves WAL immediately. Otherwise,
        /// WAL is only reserved upon connection from a streaming replication client.
        /// </param>
        /// <returns>A <see cref="NpgsqlPhysicalReplicationSlot"/> that wraps the newly-created replication slot.
        /// </returns>
        [PublicAPI]
        public async Task<NpgsqlPhysicalReplicationSlot> CreateReplicationSlot(
            string slotName,
            bool temporary = false,
            bool reserveWal = false)
        {
            var sb = new StringBuilder(" PHYSICAL");

            if (reserveWal)
                sb.Append(" RESERVE_WAL");

            var slotInfo = await CreateReplicationSlotInternal(slotName, temporary, sb.ToString());

            return new NpgsqlPhysicalReplicationSlot(this, slotInfo.SlotName, slotInfo.ConsistentPoint);
        }

        // TODO: Default for timeline -1?
        /// <summary>
        /// Instructs server to start streaming WAL, starting at WAL location <paramref name="walLocation"/>.
        /// </summary>
        /// <param name="walLocation">
        /// The WAL location from which to start streaming, in the format XXX/XXX.
        /// </param>
        /// <param name="timeline">
        /// If specified, streaming starts on that timeline; otherwise, the server's current timeline is selected.
        /// </param>
        [PublicAPI]
        public Task<IAsyncEnumerable<XLogData>> StartReplication(LogSequenceNumber walLocation,
            string? timeline = null)
            => StartReplicationInternal(walLocation);

        // ToDo: Investigate if there's a better representation for timeline than string.
        internal async Task<IAsyncEnumerable<XLogData>> StartReplicationInternal(LogSequenceNumber walLocation,
            string? slotName = null, string? timeline = null)
        {
            var sb = new StringBuilder("START_REPLICATION");

            if (slotName != null)
                sb.Append(" SLOT ").Append(slotName);

            sb.Append(" PHYSICAL ").Append(walLocation.ToString());

            if (timeline != null)
                sb.Append(' ').Append(timeline);

            var stream = await base.StartReplication(sb.ToString(), false);
            return StartStreaming(stream);

            async IAsyncEnumerable<XLogData> StartStreaming(IAsyncEnumerable<XLogData> xlogDataStream)
            {
                await foreach (var xLogData in xlogDataStream)
                    yield return xLogData;
            }
        }

        #endregion Replication commands
    }
}
