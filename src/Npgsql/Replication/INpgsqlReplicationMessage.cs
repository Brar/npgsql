﻿using JetBrains.Annotations;
using NpgsqlTypes;
using System;

namespace Npgsql.Replication
{
    /// <summary>
    /// The common base interface for all streaming replication messages
    /// </summary>
    [PublicAPI]
    public interface INpgsqlReplicationMessage
    {
        /// <summary>
        /// The starting point of the WAL data in this message.
        /// </summary>
        [PublicAPI]
        NpgsqlLogSequenceNumber WalStart { get; }

        /// <summary>
        /// The current end of WAL on the server.
        /// </summary>
        [PublicAPI]
        NpgsqlLogSequenceNumber WalEnd { get; }

        /// <summary>
        /// The server's system clock at the time this message was transmitted, as microseconds since midnight on 2000-01-01.
        /// </summary>
        [PublicAPI]
        DateTime ServerClock { get; }
    }
}