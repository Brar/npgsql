using System;
using JetBrains.Annotations;

namespace Npgsql.Replication.Logical.TestDecoding
{
    /// <summary>
    /// Text representations of PostgreSQL WAL operations decoded by the "test_decoding" plugin. See
    /// https://www.postgresql.org/docs/current/test-decoding.html.
    /// </summary>
    [PublicAPI]
    public sealed class NpgsqlTestDecodingData : IXLogBaseData
    {
        internal NpgsqlTestDecodingData(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock, string data)
        {
            WalStart = walStart;
            WalEnd = walEnd;
            ServerClock = serverClock;
            Data = data;
        }

        /// <inheritdoc />
        [PublicAPI]
        public LogSequenceNumber WalStart { get; }

        /// <inheritdoc />
        [PublicAPI]
        public LogSequenceNumber WalEnd { get; }

        /// <inheritdoc />
        [PublicAPI]
        public DateTime ServerClock { get; }

        /// <summary>
        /// Decoded text representation of the operation performed in this WAL entry
        /// </summary>
        [PublicAPI]
        public string Data { get; }
    }
}
