using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Npgsql.Replication.Logical
{
    /// <summary>
    /// Provides the base class for all classes wrapping a PostgreSQL logical replication slot.
    /// </summary>
    [PublicAPI]
    public abstract class NpgsqlLogicalReplicationSlot<T> : NpgsqlReplicationSlot where T : IXLogBaseData
    {
        /// <summary>
        /// Initializes a new instance of <see cref="NpgsqlLogicalReplicationSlot{T}"/>.
        /// </summary>
        /// <param name="connection">The replication connection to use to connect to this slot.</param>
        /// <param name="slotName">The name of the replication slot.</param>
        /// <param name="consistentPoint">The WAL location at which the slot became consistent.</param>
        /// <param name="snapshotName">The identifier of the snapshot exported by the command. The snapshot is valid
        /// until a new command is executed on this connection or the replication connection is closed.</param>
        /// <param name="outputPlugin">The name of the output plugin used by the replication slot.</param>
        [PublicAPI]
        protected NpgsqlLogicalReplicationSlot(NpgsqlReplicationConnection connection, string slotName,
            LogSequenceNumber consistentPoint, string snapshotName, string outputPlugin) : base(connection, slotName,
            consistentPoint)
        {
            SnapshotName = snapshotName;
            OutputPlugin = outputPlugin;
        }

        /// <summary>
        /// The identifier of the snapshot exported by the command.
        /// The snapshot is valid until a new command is executed on this connection or the replication connection is
        /// closed.
        /// </summary>
        [PublicAPI]
        public string SnapshotName { get; }

        /// <summary>
        /// The name of the output plugin used by the replication slot.
        /// </summary>
        [PublicAPI]
        public string OutputPlugin { get; }

        /// <summary>
        /// Instructs the server to start streaming WAL for logical replication, starting at WAL location
        /// <paramref name="walLocation"/>. The server can reply with an error, for example if the requested section of
        /// WAL has already been recycled.
        /// </summary>
        /// <param name="walLocation">The WAL location to begin streaming at.</param>
        /// <param name="options">The names and optional values of options passed to the slot's logical decoding plugin
        /// in the form of string constants.</param>
        /// <returns>A <see><cref>Task{IAsyncEnumerable{XLogData}}</cref></see> streaming WAL entries in form of
        /// <see cref="XLogData"/> values.
        /// </returns>
        [PublicAPI]
        protected Task<IAsyncEnumerable<XLogData>> StartReplicationStream(LogSequenceNumber? walLocation = null, Dictionary<string, string?>? options = null)
            => StartReplicationStream(false, walLocation, options);

        private protected async Task<IAsyncEnumerable<XLogData>> StartReplicationStream(bool bypassingStream, LogSequenceNumber? walLocation = null, Dictionary<string, string?>? options = null)
        {
            var sb = new StringBuilder("START_REPLICATION SLOT ")
                .Append(SlotName)
                .Append(" LOGICAL ")
                .Append(walLocation?.ToString() ?? ConsistentPoint.ToString());

            if (options != null)
            {
                sb
                    .Append(" (")
                    .Append(string.Join(", ", options
                        .Select(kv => kv.Value is null ? $"\"{kv.Key}\"" : $"\"{kv.Key}\" '{kv.Value}'")))
                    .Append(")");
            }

            return await Connection.StartReplication(sb.ToString(), bypassingStream);
        }
    }
}
