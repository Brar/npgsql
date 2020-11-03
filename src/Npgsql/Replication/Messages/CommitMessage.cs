﻿using NpgsqlTypes;
using System;

namespace Npgsql.Replication.Messages
{
    /// <summary>
    /// Logical Replication Protocol commit message
    /// </summary>
    public sealed class CommitMessage : LogicalReplicationProtocolMessage
    {
        /// <summary>
        /// Flags; currently unused (must be 0).
        /// </summary>
        public byte Flags { get; private set; }

        /// <summary>
        /// The LSN of the commit.
        /// </summary>
        public NpgsqlLogSequenceNumber CommitLsn { get; private set; }

        /// <summary>
        /// The end LSN of the transaction.
        /// </summary>
        public NpgsqlLogSequenceNumber TransactionEndLsn { get; private set; }

        /// <summary>
        /// Commit timestamp of the transaction.
        /// </summary>
        public DateTime TransactionCommitTimestamp { get; private set; }

        internal CommitMessage Populate(NpgsqlLogSequenceNumber walStart, NpgsqlLogSequenceNumber walEnd, DateTime serverClock, byte flags,
            NpgsqlLogSequenceNumber commitLsn, NpgsqlLogSequenceNumber transactionEndLsn, DateTime transactionCommitTimestamp)
        {
            base.Populate(walStart, walEnd, serverClock);

            Flags = flags;
            CommitLsn = commitLsn;
            TransactionEndLsn = transactionEndLsn;
            TransactionCommitTimestamp = transactionCommitTimestamp;

            return this;
        }
    }
}
