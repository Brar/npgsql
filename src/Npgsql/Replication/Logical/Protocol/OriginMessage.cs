﻿using JetBrains.Annotations;
using NpgsqlTypes;
using System;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol origin message
    /// </summary>
    [PublicAPI]
    public sealed class OriginMessage : LogicalReplicationProtocolMessage
    {
        internal OriginMessage(NpgsqlLogSequenceNumber walStart, NpgsqlLogSequenceNumber walEnd, DateTime serverClock,
            NpgsqlLogSequenceNumber originCommitLsn, string originName) : base(walStart, walEnd, serverClock)
        {
            OriginCommitLsn = originCommitLsn;
            OriginName = originName;
        }

        /// <summary>
        /// The LSN of the commit on the origin server.
        /// </summary>
        [PublicAPI]
        public NpgsqlLogSequenceNumber OriginCommitLsn { get; }

        /// <summary>
        /// Name of the origin.
        /// </summary>
        [PublicAPI]
        public string OriginName { get; }
    }
}