using System;
using System.Threading.Tasks;
using Npgsql.Replication;
using Npgsql.Replication.Logical;
using Npgsql.Replication.Logical.Raw;
using Npgsql.Replication.Physical;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    public class SafeReplicationTestBase<TConnection> : TestBase
        where TConnection : NpgsqlReplicationConnection, new()

    {
        readonly string _postfix = new TConnection() switch {
            NpgsqlLogicalReplicationConnection logical => "_l",
            NpgsqlPhysicalReplicationConnection physical => "_p",
            _ => throw new ArgumentOutOfRangeException($"{typeof(TConnection)} is not expected.")
        };

        int _max_identifier_length;

        [SetUp]
        public async Task Setup()
        {
            await using var conn = OpenConnection();
            var walLevel = (string)await conn.ExecuteScalarAsync("SHOW wal_level");
            if (walLevel != "logical")
                TestUtil.IgnoreExceptOnBuildServer("wal_level needs to be set to 'logical' in the PostgreSQL conf");
            _max_identifier_length = int.Parse((string)await conn.ExecuteScalarAsync("SHOW max_identifier_length"));
        }

        private protected async Task<TConnection> OpenReplicationConnectionAsync(NpgsqlConnectionStringBuilder? csb = null)
        {
            var c = new TConnection { ConnectionString = csb?.ToString() ?? ConnectionString };
            await c.OpenAsync();
            return c;
        }

        private protected async Task SafeReplicationTest(string baseName, Func<string, string, Task> testAction)
        {
            var maxBaseNameLength = Math.Min(baseName.Length, _max_identifier_length - 4);
            var slotName = $"s_{baseName.Substring(0, maxBaseNameLength)}{_postfix}";
            var tableName = $"t_{baseName.Substring(0, maxBaseNameLength)}{_postfix}";
            try
            {
                await testAction(slotName, tableName);
            }
            finally
            {
                await using var rc = await OpenReplicationConnectionAsync();
                try
                {
                    await rc.DropReplicationSlot(slotName);
                }
                catch (PostgresException e)
                {
                    // Temporary slots might already have been deleted
                    if (e.SqlState != "42704" || !e.Message.Contains(slotName.ToLowerInvariant()))
                    {
                        throw;
                    }
                }

                await using var c = await OpenConnectionAsync();
                await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS \"{tableName}\";");
            }
        }

        private protected async Task<NpgsqlReplicationSlot<TConnection, XLogData>> CreateReplicationSlot(
            TConnection replConn, string slotName) =>
            replConn switch
            {
                NpgsqlLogicalReplicationConnection logicalReplicationConnection => (await logicalReplicationConnection
                    .CreateReplicationSlot(slotName, "test_decoding") as NpgsqlReplicationSlot<TConnection, XLogData>)!,
                NpgsqlPhysicalReplicationConnection physicalReplicationConnection => (await physicalReplicationConnection
                    .CreateReplicationSlot(slotName) as NpgsqlReplicationSlot<TConnection, XLogData>)!,
                _ => throw new ArgumentOutOfRangeException(nameof(TConnection), replConn, "Unexpected connection type.")
            };

    }
}
