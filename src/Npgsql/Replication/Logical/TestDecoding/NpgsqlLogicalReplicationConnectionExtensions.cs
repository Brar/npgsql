using System.Threading;
using Npgsql.Replication.Logical.Internal;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Npgsql.Replication.Logical.TestDecoding
{
    /// <summary>
    /// Extension methods to use <see cref="NpgsqlLogicalReplicationConnection"/> with the
    /// test_decoding logical decoding plugin.
    /// See <a href="https://www.postgresql.org/docs/current/test-decoding.html">https://www.postgresql.org/docs/current/test-decoding.html</a>.
    /// </summary>
    [PublicAPI]
    public static class NpgsqlLogicalReplicationConnectionExtensions
    {
        /// <summary>
        /// Creates a <see cref="NpgsqlTestDecodingReplicationSlot"/> class that wraps a replication slot using the
        /// test_decoding logical decoding plugin.
        /// </summary>
        /// <remarks>
        /// See <a href="https://www.postgresql.org/docs/current/test-decoding.html">https://www.postgresql.org/docs/current/test-decoding.html</a>
        /// for more information.
        /// </remarks>
        /// <param name="connection">The <see cref="NpgsqlLogicalReplicationConnection"/> to use for creating the
        /// replication slot</param>
        /// <param name="slotName">The name of the slot to create. Must be a valid replication slot name (see
        /// <a href="https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION</a>).
        /// </param>
        /// <param name="temporarySlot">
        /// <see langword="true"/> if this replication slot shall be temporary one; otherwise <see langword="false"/>.
        /// Temporary slots are not saved to disk and are automatically dropped on error or when the session has finished.
        /// </param>
        /// <param name="slotSnapshotInitMode">
        /// A <see cref="SlotSnapshotInitMode"/> to specify what to do with the snapshot created during logical slot
        /// initialization. <see cref="SlotSnapshotInitMode.Export"/>, which is also the default, will export the
        /// snapshot for use in other sessions. This option can't be used inside a transaction.
        /// <see cref="SlotSnapshotInitMode.Use"/> will use the snapshot for the current transaction executing the
        /// command. This option must be used in a transaction, and <see cref="SlotSnapshotInitMode.Use"/> must be the
        /// first command run in that transaction. Finally, <see cref="SlotSnapshotInitMode.NoExport"/> will just use
        /// the snapshot for logical decoding as normal but won't do anything else with it.
        /// </param>
        /// <param name="cancellationToken">
        /// The token to monitor for cancellation requests.
        /// The default value is <see cref="CancellationToken.None"/>.
        /// </param>
        /// <returns>
        /// A <see cref="NpgsqlTestDecodingReplicationSlot"/> that wraps the newly-created replication slot.
        /// </returns>
        [PublicAPI]
        public static async Task<NpgsqlTestDecodingReplicationSlot> CreateReplicationSlot(
            this NpgsqlLogicalReplicationConnection connection,
            string slotName,
            bool temporarySlot = false,
            SlotSnapshotInitMode? slotSnapshotInitMode = null,
            CancellationToken cancellationToken = default)
        {
            // We don't enter NoSynchronizationContextScope here since we (have to) do it in CreateReplicationSlotForPlugin, because
            // otherwise it couldn't be set for external plugins.
            var options =
                await connection.CreateReplicationSlotForPlugin(slotName, "test_decoding", temporarySlot, slotSnapshotInitMode, cancellationToken);
            return new NpgsqlTestDecodingReplicationSlot(options);
        }
    }
}
