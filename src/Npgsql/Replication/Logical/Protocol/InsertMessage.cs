using System;
using System.Collections.Generic;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol insert message
    /// </summary>
    public sealed class InsertMessage : LogicalReplicationProtocolMessage
    {
        internal InsertMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock,
            uint relationId, TupleDataList newRow) : base(walStart, walEnd, serverClock)
        {
            RelationId = relationId;
            NewRow = newRow;
        }

        /// <summary>
        /// ID of the relation corresponding to the ID in the relation message.
        /// </summary>
        public uint RelationId { get; }

        /// <summary>
        /// Columns representing the new row.
        /// </summary>
        public IReadOnlyList<TupleData> NewRow { get; }
    }
}
