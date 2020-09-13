using System;
using System.IO;
using JetBrains.Annotations;
using NpgsqlTypes;

namespace Npgsql.Replication
{
    /// <summary>
    /// A message representing a section of the WAL data stream.
    /// </summary>
    [PublicAPI]
    public readonly struct NpgsqlXLogDataMessage
    {
        internal NpgsqlXLogDataMessage(NpgsqlLogSequenceNumber walStart, NpgsqlLogSequenceNumber walEnd, DateTime serverClock, Stream data)
        {
            WalStart = walStart;
            WalEnd = walEnd;
            ServerClock = serverClock;
            Data = data;
        }

        /// <summary>
        /// The starting point of the WAL data in this message.
        /// </summary>
        public NpgsqlLogSequenceNumber WalStart { get; }

        /// <summary>
        /// The current end of WAL on the server.
        /// </summary>
        public NpgsqlLogSequenceNumber WalEnd { get; }

        /// <summary>
        /// The server's system clock at the time this message was transmitted, as microseconds since midnight on 2000-01-01.
        /// </summary>
        public DateTime ServerClock { get; }

        /// <summary>
        /// A section of the WAL data stream that is raw WAL data in physical replication or decoded with the selected
        /// logical decoding plugin in logical replication. It is only valid until the next <see cref="NpgsqlXLogDataMessage"/>
        /// is requested from the stream.
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
