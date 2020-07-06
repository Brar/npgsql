using System;
using System.Collections.Generic;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol truncate message
    /// </summary>
    public sealed class TruncateMessage : LogicalReplicationProtocolMessage
    {
        internal TruncateMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock,
            TruncateOptions options, RelationIdList relationIds) : base(walStart, walEnd, serverClock)
        {
            Options = options;
            RelationIds = relationIds;
        }

        /// <summary>
        /// Option flags for TRUNCATE
        /// </summary>
        public TruncateOptions Options { get; }

        /// <summary>
        /// IDs of the relations corresponding to the ID in the relation message.
        /// </summary>
        public IReadOnlyList<uint> RelationIds { get; }
    }
}
