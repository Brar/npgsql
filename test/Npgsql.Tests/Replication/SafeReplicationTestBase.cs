using System;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication;
using Npgsql.Replication.Logical;
using Npgsql.Replication.Physical;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    public class SafeReplicationTestBase<TConnection> : TestBase
        where TConnection : NpgsqlReplicationConnection, new()
    {
        readonly string _postfix = new TConnection() switch {
            NpgsqlLogicalReplicationConnection _ => "_l",
            NpgsqlPhysicalReplicationConnection _ => "_p",
            _ => throw new ArgumentOutOfRangeException($"{typeof(TConnection)} is not expected.")
        };
        int _maxIdentifierLength;

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            await using var conn = await OpenConnectionAsync();
            _maxIdentifierLength = int.Parse((string)(await conn.ExecuteScalarAsync("SHOW max_identifier_length"))!);
        }

        [SetUp]
        public async Task Setup()
        {
            await using var conn = await OpenConnectionAsync();
            var walLevel = (string)(await conn.ExecuteScalarAsync("SHOW wal_level"))!;
            if (walLevel != "logical")
                TestUtil.IgnoreExceptOnBuildServer("wal_level needs to be set to 'logical' in the PostgreSQL conf");
        }

        private protected async Task<TConnection> OpenReplicationConnectionAsync(NpgsqlConnectionStringBuilder? csb = null, CancellationToken cancellationToken = default)
        {
            var c = new TConnection { ConnectionString = csb?.ToString() ?? ConnectionString };
            await c.OpenAsync(cancellationToken);
            return c;
        }

        private protected async Task SafeReplicationTest(string baseName, Func<string, string, Task> testAction)
        {
            var maxBaseNameLength = Math.Min(baseName.Length, _maxIdentifierLength - 4);
            var slotName = $"s_{baseName.Substring(0, maxBaseNameLength)}{_postfix}".ToLowerInvariant();
            var tableName = $"t_{baseName.Substring(0, maxBaseNameLength)}{_postfix}".ToLowerInvariant();
            try
            {
                await testAction(slotName, tableName);
            }
            finally
            {
                await using var c = await OpenConnectionAsync();
                try
                {
                    await c.ExecuteNonQueryAsync($"SELECT pg_drop_replication_slot('{slotName}')");
                }
                catch (PostgresException e) when (e.SqlState == "42704" && e.Message.Contains(slotName))
                {
                    // Temporary slots might already have been deleted
                    // We don't care as log as it's gone and we don't hav to clean it up
                }
                catch (PostgresException e) when (e.SqlState == "55006" && e.Message.Contains(slotName))
                {
                    // The slot is still in use. Probably because we didn't terminate
                    // the streaming replication properly.
                    // The following is ugly, but let's try to clean up after us if we can.
                    var pid = Regex.Match(e.MessageText, "PID (?<pid>\\d+)", RegexOptions.IgnoreCase).Groups["pid"];
                    if (pid.Success)
                    {
                        await c.ExecuteNonQueryAsync($"SELECT pg_terminate_backend ({pid.Value})");
                        Thread.Sleep(TimeSpan.FromSeconds(1));
                        await c.ExecuteNonQueryAsync($"SELECT pg_drop_replication_slot('{slotName}')");
                    }
                }

                await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS \"{tableName}\"");
            }
        }

        private protected static CancellationTokenSource GetCancelledCancellationTokenSource()
        {
            var cts = new CancellationTokenSource();
            cts.Cancel();
            return cts;
        }

    }
}
