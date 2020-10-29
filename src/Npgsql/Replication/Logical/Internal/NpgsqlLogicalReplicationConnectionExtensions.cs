﻿using JetBrains.Annotations;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql.Replication.Logical.Internal
{
    /// <summary>
    /// This API is for internal use and for implementing logical replication plugins.
    /// It is not meant to be consumed in common Npgsql usage scenarios.
    /// </summary>
    public static class NpgsqlLogicalReplicationConnectionExtensions
    {
        static readonly Version FirstVersionWithSlotSnapshotInitMode = new Version(10, 0);

        /// <summary>
        /// This API is for internal use and for implementing logical replication plugins.
        /// It is not meant to be consumed in common Npgsql usage scenarios.
        /// </summary>
        /// <remarks>
        /// Creates a new replication slot and returns information about the newly-created slot.
        /// </remarks>
        /// <param name="connection">The <see cref="NpgsqlLogicalReplicationConnection"/> to use for creating the
        /// replication slot</param>
        /// <param name="slotName">The name of the slot to create. Must be a valid replication slot name (see
        /// <a href="https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION</a>).
        /// </param>
        /// <param name="outputPlugin">The name of the output plugin used for logical decoding (see
        /// <a href="https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html">https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html</a>).
        /// </param>
        /// <param name="temporarySlot"><see langword="true"/> if this replication slot shall be temporary one; otherwise
        /// <see langword="false"/>. Temporary slots are not saved to disk and are automatically dropped on error or
        /// when the session has finished.</param>
        /// <param name="slotSnapshotInitMode">A <see cref="SlotSnapshotInitMode"/> to specify what to do with the
        /// snapshot created during logical slot initialization. <see cref="SlotSnapshotInitMode.Export"/>, which is
        /// also the default, will export the snapshot for use in other sessions. This option can't be used inside a
        /// transaction. <see cref="SlotSnapshotInitMode.Use"/> will use the snapshot for the current transaction
        /// executing the command. This option must be used in a transaction, and <see cref="SlotSnapshotInitMode.Use"/>
        /// must be the first command run in that transaction. Finally, <see cref="SlotSnapshotInitMode.NoExport"/> will
        /// just use the snapshot for logical decoding as normal but won't do anything else with it.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.
        /// The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>A <see cref="Task{T}"/> representing a <see cref="NpgsqlReplicationSlotOptions"/> class that
        /// can be used to initialize instances of <see cref="NpgsqlReplicationSlot"/> subclasses.</returns>
        public static Task<NpgsqlReplicationSlotOptions> CreateReplicationSlotForPlugin(
            this NpgsqlLogicalReplicationConnection connection,
            string slotName,
            string outputPlugin,
            bool temporarySlot = false,
            SlotSnapshotInitMode? slotSnapshotInitMode = null,
            CancellationToken cancellationToken = default)
        {
            using var _ = NoSynchronizationContextScope.Enter();
            return CreateReplicationSlotForPluginInternal(connection, slotName, outputPlugin, temporarySlot,
                slotSnapshotInitMode, cancellationToken);
        }

        static async Task<NpgsqlReplicationSlotOptions> CreateReplicationSlotForPluginInternal(
            this NpgsqlLogicalReplicationConnection connection,
            string slotName,
            string outputPlugin,
            bool temporarySlot,
            SlotSnapshotInitMode? slotSnapshotInitMode,
            CancellationToken cancellationToken = default)
        {
            if (slotName == null)
                throw new ArgumentNullException(nameof(slotName));
            if (outputPlugin == null)
                throw new ArgumentNullException(nameof(outputPlugin));

            try
            {
                return await connection.CreateReplicationSlotInternal(slotName, temporarySlot, commandBuilder =>
                {
                    commandBuilder.Append(" LOGICAL ").Append(outputPlugin);

                    commandBuilder.Append(slotSnapshotInitMode switch
                    {
                        // EXPORT_SNAPSHOT is the default since it has been introduced.
                        // We don't set it unless it is explicitly requested so that older backends can digest the query too.
                        null => string.Empty,
                        SlotSnapshotInitMode.Export => " EXPORT_SNAPSHOT",
                        SlotSnapshotInitMode.Use => " USE_SNAPSHOT",
                        SlotSnapshotInitMode.NoExport => " NOEXPORT_SNAPSHOT",
                        _ => throw new ArgumentOutOfRangeException(nameof(slotSnapshotInitMode),
                            slotSnapshotInitMode,
                            $"Unexpected value {slotSnapshotInitMode} for argument {nameof(slotSnapshotInitMode)}.")
                    });

                }, cancellationToken);
            }
            catch (PostgresException e)
            {
                if (connection.PostgreSqlVersion < FirstVersionWithSlotSnapshotInitMode && e.SqlState == "42601" /* syntax_error */)
                {
                    if (slotSnapshotInitMode != null)
                        throw new ArgumentException("The EXPORT_SNAPSHOT, USE_SNAPSHOT and NOEXPORT_SNAPSHOT syntax was introduced in PostgreSQL " +
                                                    $"{FirstVersionWithSlotSnapshotInitMode.ToString(1)}. Using PostgreSQL version " +
                                                    $"{connection.PostgreSqlVersion.ToString(3)} you have to omit the " +
                                                    $"{nameof(slotSnapshotInitMode)} argument.",
                            nameof(slotSnapshotInitMode), e);
                }
                throw;
            }
        }

        /// <summary>
        /// Instructs the server to start streaming the WAL for logical replication, starting at WAL location
        /// <paramref name="walLocation"/> or at the slot's consistent point if <paramref name="walLocation"/> isn't specified.
        /// The server can reply with an error, for example if the requested section of the WAL has already been recycled.
        /// </summary>
        /// <param name="connection">The <see cref="NpgsqlLogicalReplicationConnection"/> to use for starting replication</param>
        /// <param name="slot">The replication slot that will be updated as replication progresses so that the server
        /// knows which WAL segments are still needed by the standby.
        /// </param>
        /// <param name="walLocation">The WAL location to begin streaming at.</param>
        /// <param name="options">The collection of options passed to the slot's logical decoding plugin.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.
        /// The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>A <see cref="Task{T}"/> representing an <see cref="IAsyncEnumerable{T}"/> that
        /// can be used to stream WAL entries in form of <see cref="NpgsqlXLogDataMessage"/> instances.</returns>
        [PublicAPI]
        public static Task<IAsyncEnumerable<NpgsqlXLogDataMessage>> StartReplicationForPlugin(
            this NpgsqlLogicalReplicationConnection connection, NpgsqlLogicalReplicationSlot slot,
            NpgsqlLogSequenceNumber? walLocation = null, IEnumerable<KeyValuePair<string, string?>>? options = null, CancellationToken cancellationToken = default)
        {
            using var _ = NoSynchronizationContextScope.Enter();
            return connection.StartReplicationInternal(commandBuilder =>
            {
                commandBuilder.Append("SLOT ").Append(slot.SlotName).Append(' ');
                AppendCommon(commandBuilder, walLocation, options, slot.ConsistentPoint);
            }, false, cancellationToken);
        }

        internal static void AppendCommon(StringBuilder commandBuilder, NpgsqlLogSequenceNumber? walLocation, IEnumerable<KeyValuePair<string, string?>>? options, NpgsqlLogSequenceNumber consistentPoint)
        {
            commandBuilder.Append("LOGICAL ").Append(walLocation ?? consistentPoint);
            if (options != null)
            {
                using var optionsEnumerator = options.GetEnumerator();
                if (!optionsEnumerator.MoveNext())
                    return;
                commandBuilder.Append(" (");
                AppendKeyValue(commandBuilder, optionsEnumerator.Current);

                while (optionsEnumerator.MoveNext())
                {
                    commandBuilder.Append(", ");
                    AppendKeyValue(commandBuilder, optionsEnumerator.Current);
                }
                commandBuilder.Append(")");
            }

            static void AppendKeyValue(StringBuilder sb, KeyValuePair<string, string?> kv)
            {
                sb.Append('"').Append(kv.Key).Append('"');
                if(kv.Value is null)
                    return;
                sb.Append(" '").Append(kv.Value).Append('\'');
            }
        }
    }
}
