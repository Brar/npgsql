using System;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol origin message
    /// </summary>
    public sealed class OriginMessage : LogicalReplicationProtocolMessage
    {
        internal OriginMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock,
            LogSequenceNumber originCommitLsn, string originName) : base(walStart, walEnd, serverClock)
        {
            OriginCommitLsn = originCommitLsn;
            OriginName = originName;
        }

        /// <summary>
        /// The LSN of the commit on the origin server.
        /// </summary>
        public LogSequenceNumber OriginCommitLsn { get; }

        /// <summary>
        /// Name of the origin.
        /// </summary>
        public string OriginName { get; }
    }
}
