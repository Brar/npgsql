using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol update message for tables with REPLICA IDENTITY set to USING INDEX.
    /// </summary>
    public sealed class IndexUpdateMessage : UpdateMessage
    {
        internal IndexUpdateMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock,
            uint relationId, [NotNull] TupleDataList newRow, TupleDataList keyRow) : base(walStart, walEnd,
            serverClock, relationId, newRow)
            => KeyRow = keyRow;

        /// <summary>
        /// Columns representing the key.
        /// </summary>
        public IReadOnlyList<TupleData> KeyRow { get; }
    }
}
