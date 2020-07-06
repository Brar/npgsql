using JetBrains.Annotations;

namespace Npgsql.Replication
{
    /// <summary>
    /// Contains server identification information returned from <see cref="NpgsqlReplicationConnection.IdentifySystem"/>.
    /// </summary>
    [PublicAPI]
    public readonly struct NpgsqlReplicationIdentificationInfo
    {
        internal NpgsqlReplicationIdentificationInfo(
            string systemId,
            int timeline,
            string xLogPos,
            string dbName)
        {
            SystemId = systemId;
            Timeline = timeline;
            XLogPos = xLogPos;
            DbName = dbName;
        }

        /// <summary>
        /// The unique system identifier identifying the cluster.
        /// This can be used to check that the base backup used to initialize the standby came from the same cluster.
        /// </summary>
        public string SystemId { get; }

        /// <summary>
        /// Current timeline ID. Also useful to check that the standby is consistent with the master.
        /// </summary>
        public int Timeline { get; }

        /// <summary>
        /// Current WAL flush location. Useful to get a known location in the write-ahead log where streaming can start.
        /// </summary>
        public string XLogPos { get; }

        /// <summary>
        /// Database connected to or null.
        /// </summary>
        public string DbName { get; }
    }
}
