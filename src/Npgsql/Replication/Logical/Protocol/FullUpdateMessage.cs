using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol update message for tables with REPLICA IDENTITY REPLICA IDENTITY set to FULL.
    /// </summary>
    public sealed class FullUpdateMessage : UpdateMessage
    {
        internal FullUpdateMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock,
            uint relationId, [NotNull] TupleDataList newRow, TupleDataList oldRow) : base(walStart, walEnd,
            serverClock, relationId, newRow)
        {
            OldRow = oldRow;
        }

        /// <summary>
        /// Columns representing the old values.
        /// </summary>
        public IReadOnlyList<TupleData> OldRow { get; }
    }
}
