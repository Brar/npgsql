using System.Threading;
using Npgsql.Replication.Logical.Internal;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Extension methods to use <see cref="NpgsqlLogicalReplicationConnection"/> with the
    /// logical streaming replication protocol.
    /// See <a href="https://www.postgresql.org/docs/current/protocol-logical-replication.html">https://www.postgresql.org/docs/current/protocol-logical-replication.html</a>
    /// and <a href="https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html">https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html</a>.
    /// </summary>
    [PublicAPI]
    public static class NpgsqlLogicalReplicationConnectionExtensions
    {
        /// <summary>
        /// Creates a <see cref="NpgsqlPgOutputReplicationSlot"/> class that wraps a replication slot using the
        /// "pgoutput" logical decoding plugin and can be used to start streaming replication via the logical
        /// streaming replication protocol.
        /// </summary>
        /// <remarks>
        /// See <a href="https://www.postgresql.org/docs/current/protocol-logical-replication.html">https://www.postgresql.org/docs/current/protocol-logical-replication.html</a>
        /// and <a href="https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html">https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html</a>
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
        /// A <see cref="NpgsqlPgOutputReplicationSlot"/> that wraps the newly-created replication slot.
        /// </returns>
        [PublicAPI]
        public static async Task<NpgsqlPgOutputReplicationSlot> CreateReplicationSlot(
            this NpgsqlLogicalReplicationConnection connection,
            string slotName,
            bool temporarySlot = false,
            SlotSnapshotInitMode? slotSnapshotInitMode = null,
            CancellationToken cancellationToken = default)
        {
            // We don't enter NoSynchronizationContextScope here since we (have to) do it in CreateReplicationSlotForPlugin, because
            // otherwise it couldn't be set for external plugins.
            var options =
                await connection.CreateReplicationSlotForPlugin(slotName, "pgoutput", temporarySlot, slotSnapshotInitMode, cancellationToken);
            return new NpgsqlPgOutputReplicationSlot(options);
        }
    }
}
