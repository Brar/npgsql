using System;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// The base class of all Logical Replication Protocol Messages
    /// </summary>
    /// <remarks>
    /// See https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html for details about the
    /// protocol.
    /// </remarks>
    public abstract class LogicalReplicationProtocolMessage
    {
        private protected LogicalReplicationProtocolMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd,
            DateTime serverClock)
        {
            WalStart = walStart;
            WalEnd = walEnd;
            ServerClock = serverClock;
        }

        /// <summary>
        /// The starting point of the WAL data in this message.
        /// </summary>
        public LogSequenceNumber WalStart { get; }

        /// <summary>
        /// The current end of WAL on the server.
        /// </summary>
        public LogSequenceNumber WalEnd { get; }

        /// <summary>
        /// The server's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
        /// </summary>
        public DateTime ServerClock { get; }
    }
}
