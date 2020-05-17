using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

#pragma warning disable 1591

namespace Npgsql.Replication
{
    public static class NpgsqlLogicalReplicationConnectionExtensions
    {
        public static Task<NpgsqlLogicalReplicationConnection.NpgsqlLogicalReplicationSlotInfo>
            CreateOutputReplicationSlot(
                this NpgsqlLogicalReplicationConnection connection,
                string slotName,
                bool isTemporary = false,
                NpgsqlLogicalReplicationConnection.SlotSnapshotInitMode slotSnapshotInitMode =
                    NpgsqlLogicalReplicationConnection.SlotSnapshotInitMode.Export)
            => connection.CreateReplicationSlot(slotName, "pgoutput", isTemporary, slotSnapshotInitMode);

        public static async IAsyncEnumerable<OutputReplicationMessage> StartOutputReplication(
            this NpgsqlLogicalReplicationConnection connection,
            string slotName,
            string? walLocation = null,
            params string[] publicationNames)
        {
            var options = new Dictionary<string, object>
            {
                { "proto_version", "1" },
                { "publication_names", string.Join(",", publicationNames.Select(pn => $"\"{pn}\"")) }
            };

            await foreach (var xLogData in connection.StartReplication(slotName, walLocation, options))
            {

            }
        }
    }
}
