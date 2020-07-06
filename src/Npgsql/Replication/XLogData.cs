using System;
using System.IO;
using JetBrains.Annotations;

namespace Npgsql.Replication
{
    /// <summary>
    /// A message representing a section of the WAL data stream.
    /// </summary>
    [PublicAPI]
    public readonly struct XLogData
    {
        internal XLogData(
            LogSequenceNumber walStart,
            LogSequenceNumber walEnd,
            DateTime serverClock,
            Stream data)
        {
            WalStart = walStart;
            WalEnd = walEnd;
            ServerClock = serverClock;
            Data = data;
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
        /// The server's system clock at the time of transmission.
        /// </summary>
        public DateTime ServerClock { get; }

        /// <summary>
        /// A section of the WAL data stream.
        /// </summary>
        /// <remarks>
        /// A single WAL record is never split across two XLogData messages.
        /// When a WAL record crosses a WAL page boundary, and is therefore already split using continuation records,
        /// it can be split at the page boundary. In other words, the first main WAL record and its continuation
        /// records can be sent in different XLogData messages.
        /// </remarks>
        public Stream Data { get; }
    }
}
