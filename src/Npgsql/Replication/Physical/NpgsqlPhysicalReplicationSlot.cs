﻿using JetBrains.Annotations;

namespace Npgsql.Replication.Physical
{
    /// <summary>
    /// Wraps a replication slot that uses physical replication.
    /// </summary>
    public class NpgsqlPhysicalReplicationSlot : NpgsqlReplicationSlot
    {
        internal NpgsqlPhysicalReplicationSlot(string slotName)
            : base(slotName) { }
    }
}
