using Npgsql.Replication.Internal;

namespace Npgsql.Replication
{
    /// <summary>
    /// Acts as a proxy for a logical replication slot initialized for for the logical streaming replication protocol
    /// (pgoutput logical decoding plugin).
    /// </summary>
    public class NpgsqlLogicalReplicationSlot : NpgsqlLogicalReplicationSlotBase
    {
        /// <summary>
        /// Creates a new <see cref="NpgsqlLogicalReplicationSlot"/> instance.
        /// </summary>
        /// <remarks>
        /// Create a <see cref="NpgsqlLogicalReplicationSlot"/> instance with this
        /// constructor to wrap an existing PostgreSQL replication slot that has
        /// been initialized for the pgoutput logical decoding plugin.
        /// </remarks>
        /// <param name="slotName">The name of the existing replication slot</param>
        public NpgsqlLogicalReplicationSlot(string slotName)
            : this(new NpgsqlReplicationSlotOptions(slotName)) { }

        /// <summary>
        /// Creates a new <see cref="NpgsqlLogicalReplicationSlot"/> instance.
        /// </summary>
        /// <remarks>
        /// Create a <see cref="NpgsqlLogicalReplicationSlot"/> instance with this
        /// constructor to wrap an existing PostgreSQL replication slot that has
        /// been initialized for the pgoutput logical decoding plugin.
        /// </remarks>
        /// <param name="options">The <see cref="NpgsqlReplicationSlotOptions"/> representing the existing replication slot</param>
        public NpgsqlLogicalReplicationSlot(NpgsqlReplicationSlotOptions options) : base("pgoutput", options) { }

        /// <summary>
        /// Creates a new <see cref="NpgsqlLogicalReplicationSlot"/> instance.
        /// </summary>
        /// <remarks>
        /// This constructor is intended to be consumed by plugins sitting on top of
        /// <see cref="NpgsqlLogicalReplicationSlot"/>
        /// </remarks>
        /// <param name="slot">The <see cref="NpgsqlLogicalReplicationSlot"/> from which the new instance should be initialized</param>
        protected NpgsqlLogicalReplicationSlot(NpgsqlLogicalReplicationSlot slot)
            : base(slot.OutputPlugin, new NpgsqlReplicationSlotOptions(slot.SlotName, slot.ConsistentPoint, slot.SnapshotName)) { }
    }
}
