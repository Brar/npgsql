using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Npgsql.Replication
{
    /// <summary>
    /// Provides the base class for all classes wrapping a PostgreSQL replication slot.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [PublicAPI]
    public abstract class NpgsqlReplicationSlot<T>  where T : IXLogBaseData
    {
        /// <summary>
        /// Initializes a new instance of <see cref="NpgsqlReplicationSlot{T}"/>.
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

        /// <summary>
        /// Instructs the server to start streaming WAL for logical replication, starting at WAL location
        /// <paramref name="walLocation"/>. The server can reply with an error, for example if the requested section of
        /// WAL has already been recycled.
        /// </summary>
        /// <param name="walLocation">The WAL location to begin streaming at.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/> streaming WAL entries.</returns>
        public abstract Task<IAsyncEnumerable<T>> StartReplication(LogSequenceNumber? walLocation = null);
    }
}
