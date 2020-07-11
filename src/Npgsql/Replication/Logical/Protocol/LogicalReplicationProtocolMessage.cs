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
    public abstract class LogicalReplicationProtocolMessage : IXLogBaseData
    {
        private protected LogicalReplicationProtocolMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd,
            DateTime serverClock)
        {
            WalStart = walStart;
            WalEnd = walEnd;
            ServerClock = serverClock;
        }

        /// <inheritdoc />
        public LogSequenceNumber WalStart { get; }

        /// <inheritdoc />
        public LogSequenceNumber WalEnd { get; }

        /// <inheritdoc />
        public DateTime ServerClock { get; }
    }
}
