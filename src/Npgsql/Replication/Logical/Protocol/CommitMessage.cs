using System;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol commit message
    /// </summary>
    public sealed class CommitMessage : LogicalReplicationProtocolMessage
    {
        internal CommitMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock, byte flags,
            LogSequenceNumber commitLsn, LogSequenceNumber transactionEndLsn, DateTime transactionCommitTimestamp)
            : base(walStart, walEnd, serverClock)
        {
            Flags = flags;
            CommitLsn = commitLsn;
            TransactionEndLsn = transactionEndLsn;
            TransactionCommitTimestamp = transactionCommitTimestamp;
        }

        /// <summary>
        /// Flags; currently unused (must be 0).
        /// </summary>
        public byte Flags { get; }

        /// <summary>
        /// The LSN of the commit.
        /// </summary>
        public LogSequenceNumber CommitLsn { get; }

        /// <summary>
        /// The end LSN of the transaction.
        /// </summary>
        public LogSequenceNumber TransactionEndLsn { get; }

        /// <summary>
        /// Commit timestamp of the transaction.
        /// </summary>
        public DateTime TransactionCommitTimestamp { get; }
    }
}
