using System;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol begin message
    /// </summary>
    public sealed class BeginMessage : LogicalReplicationProtocolMessage
    {
        internal BeginMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock,
            LogSequenceNumber transactionFinalLsn, DateTime transactionCommitTimestamp, uint transactionXid)
            : base(walStart, walEnd, serverClock)
        {
            TransactionFinalLsn = transactionFinalLsn;
            TransactionCommitTimestamp = transactionCommitTimestamp;
            TransactionXid = transactionXid;
        }

        /// <summary>
        /// The final LSN of the transaction.
        /// </summary>
        public LogSequenceNumber TransactionFinalLsn { get; }

        /// <summary>
        /// Commit timestamp of the transaction.
        /// The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
        /// </summary>
        public DateTime TransactionCommitTimestamp { get; }

        /// <summary>
        /// Xid of the transaction.
        /// </summary>
        public uint TransactionXid { get; }
    }
}
