using System;
using System.Collections.Generic;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol delete message for tables with REPLICA IDENTITY REPLICA IDENTITY set to FULL.
    /// </summary>
    public sealed class FullDeleteMessage : DeleteMessage
    {
        internal FullDeleteMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock,
            uint relationId, TupleDataList oldRow) : base(walStart, walEnd, serverClock, relationId)
            => OldRow = oldRow;

        /// <summary>
        /// Columns representing the old values.
        /// </summary>
        public IReadOnlyList<TupleData> OldRow { get; }
    }
}
