using System;

namespace Npgsql.Replication
{
    /// <summary>
    /// The base interface of all Streaming Replication Messages
    /// </summary>
    public interface IXLogBaseData
    {
        /// <summary>
        /// The starting point of the WAL data in this message.
        /// </summary>
        LogSequenceNumber WalStart { get; }

        /// <summary>
        /// The current end of WAL on the server.
        /// </summary>
        LogSequenceNumber WalEnd { get; }

        /// <summary>
        /// The server's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
        /// </summary>
        DateTime ServerClock { get; }
    }
}
