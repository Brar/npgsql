using JetBrains.Annotations;
using NpgsqlTypes;

namespace Npgsql.Replication.Physical
{
    /// <summary>
    /// Wraps a replication slot that uses physical replication.
    /// </summary>
    [PublicAPI]
    public class NpgsqlPhysicalReplicationSlot : NpgsqlReplicationSlot
    {
        internal NpgsqlPhysicalReplicationSlot(string slotName)
            : base(slotName) { }
    }
}
