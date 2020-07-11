using JetBrains.Annotations;

namespace Npgsql.Replication
{
    /// <summary>
    /// Provides the base class for all classes wrapping a PostgreSQL replication slot.
    /// </summary>
    [PublicAPI]
    public abstract class NpgsqlReplicationSlot
    {
        /// <summary>
        /// Initializes a new instance of <see cref="NpgsqlReplicationSlot"/>.
        /// </summary>
        /// <param name="connection">The replication connection to use to connect to this slot.</param>
        /// <param name="slotName">The name of the replication slot.</param>
        /// <param name="consistentPoint">The WAL location at which the slot became consistent.</param>
        private protected NpgsqlReplicationSlot(NpgsqlReplicationConnection connection, string slotName, LogSequenceNumber consistentPoint)
        {
            Connection = connection;
            SlotName = slotName;
            ConsistentPoint = consistentPoint;
        }

        private protected NpgsqlReplicationConnection Connection { get; }

        /// <summary>
        /// The name of the replication slot.
        /// </summary>
        [PublicAPI]
        public string SlotName { get; }

        /// <summary>
        /// The WAL location at which the slot became consistent.
        /// This is the earliest location from which streaming can start on this replication slot.
        /// </summary>
        [PublicAPI]
        public LogSequenceNumber ConsistentPoint { get; }
    }
}
