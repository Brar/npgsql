using System;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.Replication.Internal;

namespace Npgsql.Replication.Logical.Internal
{
    /// <summary>
    /// This API is for internal use and for implementing logical replication plugins.
    /// It is not meant to be consumed in common Npgsql usage scenarios.
    /// </summary>
    public static class NpgsqlLogicalReplicationConnectionExtensions
    {
        static readonly Version V10_0 = new Version(10, 0);

        /// <summary>
        /// This API is for internal use and for implementing logical replication plugins.
        /// It is not meant to be consumed in common Npgsql usage scenarios.
        /// </summary>
        public static async Task<NpgsqlReplicationSlotInfo> CreateReplicationSlotForPlugin(
            this NpgsqlLogicalReplicationConnection connection,
            string slotName,
            string outputPlugin,
            bool temporary = false,
            SlotSnapshotInitMode? slotSnapshotInitMode = null)
        {
            var sb = new StringBuilder(" LOGICAL ").Append(outputPlugin);

            sb.Append(slotSnapshotInitMode switch
            {
                // EXPORT_SNAPSHOT is the default.
                // We don't set it unless it is explicitly requested so that older backends can digest the query too.
                null => string.Empty,
                SlotSnapshotInitMode.Export => " EXPORT_SNAPSHOT",
                SlotSnapshotInitMode.Use => " USE_SNAPSHOT",
                SlotSnapshotInitMode.NoExport => " NOEXPORT_SNAPSHOT",
                _ => throw new ArgumentOutOfRangeException(nameof(slotSnapshotInitMode),
                    slotSnapshotInitMode,
                    $"Unexpected value {slotSnapshotInitMode} for argument {nameof(slotSnapshotInitMode)}.")
            });
            try
            {
                return await connection.CreateReplicationSlotInternal(slotName, temporary, sb.ToString());
            }
            catch (PostgresException e)
            {
                if (connection.Connection.PostgreSqlVersion < V10_0 && e.SqlState == "42601" /* syntax_error */)
                {
                    if (slotSnapshotInitMode != SlotSnapshotInitMode.Export)
                        throw new ArgumentException($"The USE_SNAPSHOT and NOEXPORT_SNAPSHOT syntax was introduced in PostgreSQL {V10_0.ToString(1)}. Using PostgreSQL version {connection.Connection.PostgreSqlVersion.ToString(3)} you have to set the {nameof(slotSnapshotInitMode)} argument to {nameof(SlotSnapshotInitMode)}.{nameof(SlotSnapshotInitMode.NoExport)}.", nameof(slotSnapshotInitMode), e);
                }
                throw;
            }
        }
    }
}
