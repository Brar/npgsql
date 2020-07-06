using JetBrains.Annotations;

namespace Npgsql.Replication.Physical
{
    /// <summary>
    /// Contains information about a newly-created physical replication slot.
    /// </summary>
    [PublicAPI]
    public readonly struct NpgsqlPhysicalReplicationSlotInfo
    {
        internal NpgsqlPhysicalReplicationSlotInfo(string slotName, string consistentPoint)
        {
            SlotName = slotName;
            ConsistentPoint = consistentPoint;
        }

        /// <summary>
        /// The name of the newly-created replication slot.
        /// </summary>
        public string SlotName { get; }

        /// <summary>
        /// The WAL location at which the slot became consistent.
        /// This is the earliest location from which streaming can start on this replication slot.
        /// </summary>
        public string ConsistentPoint { get; }
    }
}
