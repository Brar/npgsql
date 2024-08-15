using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.Internal;
using static Npgsql.Util.Statics;

namespace Npgsql.Replication;

/// <summary>
///
/// </summary>
public sealed class NpgsqlLogicalReplicationExporter(NpgsqlConnector connector) : IAsyncEnumerable<NpgsqlLogicalReplicationExportRow>, IAsyncDisposable
{
    string _copyToCommand = null!;
    internal void Init(string copyToCommand)
        => _copyToCommand = copyToCommand;

    /// <inheritdoc />
    public async IAsyncEnumerator<NpgsqlLogicalReplicationExportRow> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        await connector.WriteQuery(_copyToCommand, true, cancellationToken).ConfigureAwait(false);
        await connector.Flush(true, cancellationToken).ConfigureAwait(false);
        var copyOutResponseMessage = Expect<CopyOutResponseMessage>(await connector.ReadMessage(true).ConfigureAwait(false), connector);
        var columnFormatCodes = copyOutResponseMessage.ColumnFormatCodes;
        while (true)
        {
            var msg = await connector.ReadMessage(true).ConfigureAwait(false);
            switch (msg)
            {
            case CopyDataMessage copyData:
                yield return new NpgsqlLogicalReplicationExportRow();
                continue;
            case CopyDoneMessage copyDone:
                yield break;
            }
            Expect<ReadyForQueryMessage>(await connector.ReadMessage(true).ConfigureAwait(false), connector);
            break;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        var e = GetAsyncEnumerator();
        while (await e.MoveNextAsync().ConfigureAwait(false))
        {
            // Just consume the stream
        }
    }
}

/// <summary>
///
/// </summary>
public readonly struct NpgsqlLogicalReplicationExportRow()
{

}
