using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.Internal;
using static Npgsql.Util.Statics;

namespace Npgsql.Replication;

/// <summary>
/// Represents a logical replication connection to a PostgreSQL server.
/// </summary>
public sealed class LogicalReplicationConnection : ReplicationConnection
{
    NpgsqlLogicalReplicationExporter _exporter = null!;
    private protected override ReplicationMode ReplicationMode => ReplicationMode.Logical;

    /// <summary>
    /// Initializes a new instance of <see cref="LogicalReplicationConnection"/>.
    /// </summary>
    public LogicalReplicationConnection() {}

    /// <summary>
    /// Initializes a new instance of <see cref="LogicalReplicationConnection"/> with the given connection string.
    /// </summary>
    /// <param name="connectionString">The connection used to open the PostgreSQL database.</param>
    public LogicalReplicationConnection(string? connectionString) : base(connectionString) {}

    /// <summary>
    /// Begins a binary COPY TO STDOUT operation, a high-performance data export mechanism from a PostgreSQL table.
    /// </summary>
    /// <param name="copyToCommand">A COPY TO STDOUT SQL command</param>
    /// <param name="cancellationToken">An optional token to cancel the asynchronous operation. The default value is None.</param>
    /// <returns>A <see cref="NpgsqlBinaryExporter"/> which can be used to read rows and columns</returns>
    /// <remarks>
    /// See https://www.postgresql.org/docs/current/static/sql-copy.html.
    /// </remarks>
    public NpgsqlLogicalReplicationExporter BeginBinaryExport(string copyToCommand, CancellationToken cancellationToken = default)
    {
        if (copyToCommand == null)
            throw new ArgumentNullException(nameof(copyToCommand));
        if (!IsValidCopyCommand(copyToCommand))
            throw new ArgumentException("Must contain a COPY TO STDOUT command!", nameof(copyToCommand));

        CheckDisposed();

        using var _ = Connector.StartUserAction(ConnectorState.Copy, cancellationToken, attemptPgCancellation: PgCancellationSupported);
        LogMessages.ExecutingReplicationCommand(ReplicationLogger, copyToCommand, Connector.Id);
        _exporter ??= new(Connector);
        _exporter.Init(copyToCommand);

        return _exporter;

        static bool IsValidCopyCommand(string copyCommand)
            => copyCommand.AsSpan().TrimStart().StartsWith("COPY", StringComparison.OrdinalIgnoreCase);
    }
}
