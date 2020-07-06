using System;
using System.Collections.Generic;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol update message for tables with REPLICA IDENTITY set to DEFAULT.
    /// </summary>
    /// <remarks>
    /// This is the base type of all update messages containing only the tuples for the new row.
    /// </remarks>
    public class UpdateMessage : LogicalReplicationProtocolMessage
    {
        internal UpdateMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock,
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
        public IReadOnlyList<TupleData>  NewRow { get; }
    }
}
