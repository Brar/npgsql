namespace Npgsql.Replication.Internal
{
    /// <summary>
    /// Contains information about a newly-created logical replication slot.
    /// </summary>
    public readonly struct NpgsqlReplicationSlotInfo
    {
        internal NpgsqlReplicationSlotInfo(string slotName, string consistentPoint, string? snapshotName, string? outputPlugin)
        {
            SlotName = slotName;
            ConsistentPoint = LogSequenceNumber.Parse(consistentPoint);
            SnapshotName = snapshotName;
            OutputPlugin = outputPlugin;
        }

        /// <summary>
        /// The name of the newly-created replication slot.
        /// </summary>
        public string SlotName { get; }

        /// <summary>
        /// The WAL location at which the slot became consistent.
        /// This is the earliest location from which streaming can start on this replication slot.
        /// </summary>
        public LogSequenceNumber ConsistentPoint { get; }

        /// <summary>
        /// The identifier of the snapshot exported by the command.
        /// The snapshot is valid until a new command is executed on this connection or the replication connection is closed.
        /// </summary>
        public string? SnapshotName { get; }

        /// <summary>
        /// The name of the output plugin used by the newly-created replication slot.
        /// </summary>
        public string? OutputPlugin { get; }
    }
}
