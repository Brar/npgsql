using System;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.Replication.Logical;
using Npgsql.Replication.Logical.TestDecoding;

namespace Npgsql.Replication.Physical
{
    /// <summary>
    /// Extension methods to use <see cref="NpgsqlPhysicalReplicationConnection"/> with the
    /// <see cref="NpgsqlPhysicalReplicationSlot"/> class.
    /// </summary>
    [PublicAPI]
    public static class NpgsqlPhysicalReplicationConnectionExtensions
    {
        /// <summary>
        /// Creates a <see cref="NpgsqlPhysicalReplicationSlot"/> that wraps a replication slot using the
        /// "test_decoding" plugin and can be used to start streaming replication using
        /// <see cref="NpgsqlTestDecodingData"/> messages.
        /// </summary>
        /// <param name="connection">The <see cref="NpgsqlPhysicalReplicationConnection"/> to use for creating the
        /// replication slot</param>
        /// <param name="slotName">The name of the slot to create.</param>
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
        public static async Task<NpgsqlPhysicalReplicationSlot> CreateReplicationSlot(
            this NpgsqlPhysicalReplicationConnection connection,
            string slotName,
            bool temporary = false,
            bool reserveWal = false)
        {
            var sb = new StringBuilder(" PHYSICAL");

            if (reserveWal)
                sb.Append(" RESERVE_WAL");

            var slotInfo = await connection.CreateReplicationSlotInternal(slotName, temporary, sb.ToString());

            return new NpgsqlPhysicalReplicationSlot(connection, slotInfo.SlotName, slotInfo.ConsistentPoint);
        }
    }
}
