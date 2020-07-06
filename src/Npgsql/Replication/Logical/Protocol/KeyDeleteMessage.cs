using System;
using System.Collections.Generic;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol delete message for tables with REPLICA IDENTITY set to DEFAULT or USING INDEX.
    /// </summary>
    public sealed class KeyDeleteMessage : DeleteMessage
    {
        internal KeyDeleteMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock,
            uint relationId, TupleDataList keyRow) : base(walStart, walEnd, serverClock, relationId)
            => KeyRow = keyRow;

        /// <summary>
        /// Columns representing the primary key.
        /// </summary>
        public IReadOnlyList<TupleData> KeyRow { get; }
    }
}
