using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication;
using Npgsql.Tests.Support;
using NpgsqlTypes;
using NUnit.Framework;

namespace Npgsql.Tests.Replication;

/// <summary>
/// Tests replication connections against a mocked walsender. This covers server behavior that is hard or impossible to
/// trigger reliably on a real server, like timeline switches, shutdowns or protocol edge cases.
/// </summary>
public class ReplicationMockTests : TestBase
{
    [Test]
    public async Task Logical_startup_and_IDENTIFY_SYSTEM()
    {
        await using var postmaster = PgPostmasterMock.Start(ConnectionString);
        var (rc, server) = await postmaster.OpenReplicationConnection<LogicalReplicationConnection>();
        await using var _ = rc;
        using var cts = new CancellationTokenSource(ReplicationMock.Timeout);

        Assert.That(server.StartupParameters["replication"], Is.EqualTo("database"));

        var identifyTask = rc.IdentifySystem(cts.Token);
        await server.ExpectSimpleQuery("IDENTIFY_SYSTEM").WaitAsync(ReplicationMock.Timeout);
        await server
            .WriteIdentifySystemResponse("7000000000000000001", 1, new NpgsqlLogSequenceNumber(0x1000000), "npgsql_tests")
            .FlushAsync();

        var info = await identifyTask;
        Assert.That(info.SystemId, Is.EqualTo("7000000000000000001"));
        Assert.That(info.Timeline, Is.EqualTo(1u));
        Assert.That(info.XLogPos, Is.EqualTo(new NpgsqlLogSequenceNumber(0x1000000)));
        Assert.That(info.DbName, Is.EqualTo("npgsql_tests"));
    }

    [Test]
    public async Task Physical_streaming_and_cancellation()
    {
        await using var postmaster = PgPostmasterMock.Start(ConnectionString);
        var (rc, server) = await postmaster.OpenReplicationConnection<PhysicalReplicationConnection>();
        await using var _ = rc;
        using var timeoutCts = new CancellationTokenSource(ReplicationMock.Timeout);
        using var streamingCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token);

        Assert.That(server.StartupParameters["replication"], Is.EqualTo("true"));

        var startLsn = new NpgsqlLogSequenceNumber(0x1000000);
        var dataEndLsn = new NpgsqlLogSequenceNumber(0x1000003);
        var enumerator = rc.StartReplication(startLsn, streamingCts.Token).GetAsyncEnumerator();
        var moveNext = enumerator.MoveNextAsync().AsTask();
        try
        {
            await server.ExpectSimpleQuery("START_REPLICATION PHYSICAL 0/1000000").WaitAsync(ReplicationMock.Timeout);
            await server
                .WriteCopyBothResponse()
                .WriteXLogData(startLsn, dataEndLsn, "abc"u8)
                .FlushAsync();

            Assert.That(await moveNext, Is.True);
            var message = enumerator.Current;
            Assert.That(message.WalStart, Is.EqualTo(startLsn));
            Assert.That(message.WalEnd, Is.EqualTo(dataEndLsn));
            var data = new byte[3];
            await message.Data.ReadExactlyAsync(data, timeoutCts.Token);
            Assert.That(data, Is.EqualTo("abc"u8.ToArray()));

            // Notices don't disturb the stream, and a keepalive requesting a reply gets an immediate status update
            moveNext = enumerator.MoveNextAsync().AsTask();
            await server
                .WriteNoticeResponse(PostgresErrorCodes.Warning)
                .WriteKeepalive(dataEndLsn, replyRequested: true)
                .FlushAsync();
            var statusUpdate = await server.ExpectStandbyStatusUpdate().WaitAsync(ReplicationMock.Timeout);
            Assert.That(statusUpdate.WriteLsn, Is.EqualTo(dataEndLsn));
            Assert.That(statusUpdate.ReplyRequested, Is.False);

            // Cancelling the token cancels START_REPLICATION with a PostgreSQL cancel request
            streamingCts.Cancel();
            await postmaster.WaitForCancellationRequest().AsTask().WaitAsync(ReplicationMock.Timeout);
            await server
                .WriteErrorResponse(PostgresErrorCodes.QueryCanceled)
                .WriteReadyForQuery()
                .FlushAsync();
            Assert.That(async () => await moveNext, Throws.InstanceOf<OperationCanceledException>());
        }
        finally
        {
            if (!moveNext.IsCompleted)
            {
                // A pending MoveNextAsync would make DisposeAsync throw and hide the actual failure, so end it first
                server.Close();
                try
                {
                    await moveNext;
                }
                catch
                {
                    // The test has already failed
                }
            }

            await enumerator.DisposeAsync();
        }

        // The connection can be used for further commands
        var identifyTask = rc.IdentifySystem(timeoutCts.Token);
        await server.ExpectSimpleQuery("IDENTIFY_SYSTEM").WaitAsync(ReplicationMock.Timeout);
        await server
            .WriteIdentifySystemResponse("7000000000000000001", 1, dataEndLsn, dbName: null)
            .FlushAsync();
        var info = await identifyTask;
        Assert.That(info.XLogPos, Is.EqualTo(dataEndLsn));
        Assert.That(info.DbName, Is.Null);
    }

    [Test, Description("Walsenders before PostgreSQL 10 accept only replication commands, so Open can't load the types there")]
    public async Task Open_before_PostgreSQL_10([Values] bool physical)
    {
        await using var postmaster = PgPostmasterMock.Start(ConnectionString, serverVersion: "9.6.24");
        var connectionString = WithTypeLoading(postmaster.ConnectionString);
        await using ReplicationConnection rc = physical
            ? new PhysicalReplicationConnection(connectionString)
            : new LogicalReplicationConnection(connectionString);
        using var cts = new CancellationTokenSource(ReplicationMock.Timeout);

        var openTask = rc.Open(cts.Token);
        var server = await postmaster.WaitForServerConnection().AsTask().WaitAsync(ReplicationMock.Timeout);
        var queryTask = server.WaitForData();
        if (await Task.WhenAny(openTask, queryTask).WaitAsync(ReplicationMock.Timeout) == queryTask)
        {
            // Open sent a query: reject it like a walsender before PostgreSQL 10
            await server.ExpectMessage(FrontendMessageCode.Query);
            await server
                .WriteErrorResponse(PostgresErrorCodes.SyntaxError, "ERROR", "syntax error")
                .WriteReadyForQuery()
                .FlushAsync();
        }

        await openTask;
        Assert.That(rc.PostgreSqlVersion, Is.EqualTo(new Version(9, 6, 24)));

        // The first query is IDENTIFY_SYSTEM, which ends the read that WaitForData started
        var identifyTask = rc.IdentifySystem(cts.Token);
        await queryTask.WaitAsync(ReplicationMock.Timeout);
        await server.ExpectSimpleQuery("IDENTIFY_SYSTEM").WaitAsync(ReplicationMock.Timeout);
        await server
            .WriteIdentifySystemResponse("7000000000000000001", 1, new NpgsqlLogSequenceNumber(0x1000000), physical ? null : "npgsql_tests")
            .FlushAsync();
        Assert.That((await identifyTask).Timeline, Is.EqualTo(1u));
    }

    [Test, Description("Logical replication connections to PostgreSQL 10 or later still load the types")]
    public async Task Logical_open_loads_types_from_PostgreSQL_10()
    {
        await using var postmaster = PgPostmasterMock.Start(ConnectionString, serverVersion: "10.23");
        await using var rc = new LogicalReplicationConnection(WithTypeLoading(postmaster.ConnectionString));
        using var cts = new CancellationTokenSource(ReplicationMock.Timeout);

        var openTask = rc.Open(cts.Token);
        var server = await postmaster.WaitForServerConnection().AsTask().WaitAsync(ReplicationMock.Timeout);
        var queryTask = server.WaitForData();
        Assert.That(await Task.WhenAny(openTask, queryTask).WaitAsync(ReplicationMock.Timeout), Is.SameAs(queryTask),
            "Open completed without loading the types");
        await server.ExpectSimpleQuery("SELECT version();").WaitAsync(ReplicationMock.Timeout);

        // The mock doesn't implement type loading
        server.Close();
        Assert.That(async () => await openTask, Throws.InstanceOf<NpgsqlException>());
    }

    // The mock's connection string disables type loading
    static string WithTypeLoading(string connectionString)
#pragma warning disable CS0618 // Type or member is obsolete
        => new NpgsqlConnectionStringBuilder(connectionString) { ServerCompatibilityMode = ServerCompatibilityMode.None }.ToString();
#pragma warning restore CS0618 // Type or member is obsolete
}
