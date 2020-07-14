using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.Replication.Logical;
using Npgsql.Replication.Physical;

namespace Npgsql.Replication
{
    /// <summary>
    /// Provides the base class for all classes wrapping a PostgreSQL replication slot.
    /// </summary>
    /// <typeparam name="TConnection">The <see cref="NpgsqlReplicationConnection"/> subclass this slot uses. Either
    /// <see cref="NpgsqlPhysicalReplicationConnection"/> or <see cref="NpgsqlLogicalReplicationConnection"/>.
    /// </typeparam>
    /// <typeparam name="TStream">The type of replication message stream this slot produces. The common denominator for
    /// these kind of messages is <see cref="IXLogBaseData"/>.</typeparam>
    [PublicAPI]
    public abstract class NpgsqlReplicationSlot<TConnection, TStream>
        where TConnection : NpgsqlReplicationConnection
        where TStream : IXLogBaseData
    {
        /// <summary>
        /// Initializes a new instance of <see cref="NpgsqlReplicationSlot{TConnection, TStream}"/>.
        /// </summary>
        /// <param name="connection">The replication connection to use to connect to this slot.</param>
        /// <param name="slotName">The name of the replication slot.</param>
        /// <param name="consistentPoint">The WAL location at which the slot became consistent.</param>
        private protected NpgsqlReplicationSlot(TConnection connection, string slotName, LogSequenceNumber consistentPoint)
        {
            Connection = connection;
            SlotName = slotName;
            ConsistentPoint = consistentPoint;
        }

        private protected TConnection Connection { get; }

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
        /// Instructs the server to start streaming WAL for replication, starting at WAL location
        /// <paramref name="walLocation"/>. The server can reply with an error, for example if the requested section of
        /// WAL has already been recycled. Some logical decoding plugins require additional options to be specified when
        /// starting replication. In this case you will hve to use another overload of this method
        /// </summary>
        /// <param name="walLocation">The WAL location to begin streaming at.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/> streaming WAL entries.</returns>
        public abstract Task<IAsyncEnumerable<TStream>> StartReplication(LogSequenceNumber? walLocation = null);
    }
}
