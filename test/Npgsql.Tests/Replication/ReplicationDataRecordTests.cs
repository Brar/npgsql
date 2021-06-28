using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    [TestFixture(ReplicationDataMode.DefaultReplicationData, Buffering.Unbuffered, AsyncMode.Async)]
    [TestFixture(ReplicationDataMode.BinaryReplicationData, Buffering.Unbuffered, AsyncMode.Async)]
    [TestFixture(ReplicationDataMode.DefaultReplicationData, Buffering.Unbuffered, AsyncMode.Sync)]
    [TestFixture(ReplicationDataMode.BinaryReplicationData, Buffering.Unbuffered, AsyncMode.Sync)]
    [TestFixture(ReplicationDataMode.DefaultReplicationData, Buffering.Buffered, AsyncMode.Async)]
    [TestFixture(ReplicationDataMode.BinaryReplicationData, Buffering.Buffered, AsyncMode.Async)]
    [TestFixture(ReplicationDataMode.DefaultReplicationData, Buffering.Buffered, AsyncMode.Sync)]
    [TestFixture(ReplicationDataMode.BinaryReplicationData, Buffering.Buffered, AsyncMode.Sync)]
    public class ReplicationDataRecordTests : PgOutputReplicationTestBase
    {
        #region GetFieldValueTestCases

        static IEnumerable GetFieldValueTestCases()
        {
            foreach (var t in TestCases())
            {
                var testCase = new TestCaseData(t.TypeName, t.BinaryValue, t.StringValue, t.InputString).SetName(t.TestCaseName);
                testCase = t.ExplicitReason is null ? testCase : testCase.Explicit(t.ExplicitReason);
                testCase = t.IgnoreReason is null ? testCase : testCase.Ignore(t.IgnoreReason);
                yield return testCase;
            }

            static IEnumerable<GetFieldValueTestCase> TestCases()
            {
                // Numeric Types
                yield return new("smallint", (short)42);
                yield return new("smallint[]", new short[]{1,2});
                yield return new("integer", 42);
                yield return new("integer[]", new []{1,2});
                yield return new("bigint", 42L);
                yield return new("bigint[]", new []{1L,2L});
                yield return new("numeric", 42.42M);
                yield return new("numeric[]", new []{1.1M,2.2M});
                yield return new("real", 42.42f);
                yield return new("real[]", new []{1.1f,2.2f});
                yield return new("double precision", 42.42D);
                yield return new("double precision[]", new []{1.1D,2.2D});

                // Monetary Types
                yield return new("money", 42.42M, "$42.42");
                yield return new("money[]", new []{1.1M,2.2M}, "{$1.10,$2.20}");

                // Character Types
                yield return new("character varying(10)", "Test");
                yield return new("character varying(10)[]", new []{"Test 1","Test 2"}, "{\"Test 1\",\"Test 2\"}");
                yield return new("character(4)", "Test");
                yield return new("character(6)[]", new []{"Test 1","Test 2"}, "{\"Test 1\",\"Test 2\"}");
                yield return new("text", "Test");
                yield return new("text[]", new []{"Test 1","Test 2"}, "{\"Test 1\",\"Test 2\"}");
                yield return new("\"char\"", 'T');
                yield return new("\"char\"[]", new []{'A','B'});
                yield return new("name", "Test");
                yield return new("name[]", new []{"Test 1","Test 2"}, "{\"Test 1\",\"Test 2\"}");

                // Binary Data Types
                static byte[] B(string text) => Encoding.UTF8.GetBytes(text);
                yield return new("bytea", B("Test"), "Test");
                yield return new("bytea[]", new []{B("Test 1"), B("Test 2")});

                // Date/Time Types
                var ts = new DateTime(2021, 06, 25, 19, 16, 48, DateTimeKind.Unspecified);
                yield return new("timestamp", ts);
                yield return new("timestamp[]", new []{ts, ts}, "{\"2021-06-25 19:16:48\",\"2021-06-25 19:16:48\"}");
                var tstz = new DateTime(2021, 06, 25, 21, 16, 48, DateTimeKind.Local);
                yield return new("timestamp with time zone", tstz);
                yield return new("timestamp with time zone[]", new []{tstz, tstz}, "{\"2021-06-25 21:16:48+02\",\"2021-06-25 21:16:48+02\"}");
                var date = new DateTime(2021, 06, 25);
                yield return new("date", date);
                yield return new("date[]", new []{date, date});
                var time = new TimeSpan(19, 16, 48);
                yield return new("time", time);
                yield return new("time[]", new []{time, time});
                // Mind you, the date has to be 0001-01-02 for the test to pass since that's the date we adjust
                // the returned DateTimeOffset to.
                var timetz = new DateTimeOffset(1, 1, 2, 19, 16, 48, TimeSpan.FromHours(2));
                yield return new("time with time zone", timetz);
                yield return new("time with time zone[]", new []{timetz, timetz});
                var interval = new TimeSpan(456, 17, 6, 34);
                yield return new("interval", interval);
                yield return new("interval[]", new []{interval, interval}, "{\"42 years 3 mons 24 days 19:16:48\",\"42 years 3 mons 24 days 19:16:48\"}");

                // Boolean Type
                yield return new("bool", true, "t");
                yield return new("bool[]", new []{true, false}, "{t,f}");

                // Enumerated Types
                yield return new(nameof(RDRTGFVEnum), RDRTGFVEnum.Sad);
                yield return new($"{nameof(RDRTGFVEnum)}[]", new []
                {
                    RDRTGFVEnum.Ok, RDRTGFVEnum.Happy
                });

                // Geometric Types
                var point = new NpgsqlPoint(4.2D, 42D);
                yield return new("point", point);
                yield return new("point[]", new []{point, point}, "{\"(4.2,42)\",\"(4.2,42)\"}");
                var line = new NpgsqlLine(0.42D, 4.2D, 42D);
                yield return new("line", line);
                yield return new("line[]", new []{line, line}, "{\"{0.42,4.2,42}\",\"{0.42,4.2,42}\"}");
                var lseg = new NpgsqlLSeg(0.42D, 4.2D, 42D, 420D);
                yield return new("lseg", lseg);
                yield return new("lseg[]", new []{lseg, lseg}, "{\"[(0.42,4.2),(42,420)]\",\"[(0.42,4.2),(42,420)]\"}");
                var box = new NpgsqlBox(420D, 42D, 4.2D, 0.42D);
                yield return new("box", box);
                yield return new("box[]", new []{box, box}, "{(420,42),(4.2,0.42);(420,42),(4.2,0.42)}");
                var path = new NpgsqlPath(new NpgsqlPoint(0.42D, 4.2D), new NpgsqlPoint(42D, 420D));
                yield return new("path", path);
                yield return new("path[]", new []{path, path}, "{\"((420,42),(4.2,0.42))\",\"((420,42),(4.2,0.42))\"}");
                var polygon = new NpgsqlPolygon(new NpgsqlPoint(0.42D, 4.2D), new NpgsqlPoint(42D, 420D));
                yield return new("polygon", polygon);
                yield return new("polygon[]", new []{polygon, polygon}, "{\"((420,42),(4.2,0.42))\",\"((420,42),(4.2,0.42))\"}");
                var circle = new NpgsqlCircle(0.42D, 4.2D, 42D);
                yield return new("circle", circle);
                yield return new("circle[]", new []{circle, circle}, "{\"<(0.42,4.2),42>\",\"<(0.42,4.2),42>\"}");

                // Network Address Types
                var inet = IPAddress.Parse("127.0.0.1");
                yield return new("inet", inet);
                yield return new("inet[]", new []{inet, inet});
                var cidr = (IPAddress.Parse("2001:4f8:3:ba:2e0:81ff:fe22:d1f1"), 128);
                yield return new("cidr", cidr, testCaseName: $"{nameof(GetFieldValue)}<({nameof(IPAddress)},{nameof(Int32)})>({{0}})");
                yield return new("cidr[]", new []{cidr, cidr}, testCaseName: $"{nameof(GetFieldValue)}<({nameof(IPAddress)},{nameof(Int32)})[]>({{0}})");
                var macaddr = PhysicalAddress.Parse("08-00-2B-01-02-03");
                yield return new("macaddr", macaddr);
                yield return new("macaddr[]", new []{macaddr, macaddr});
                var macaddr8 = PhysicalAddress.Parse("08-00-2B-01-02-03-04-05");
                yield return new("macaddr8", macaddr8);
                yield return new("macaddr8[]", new []{macaddr8, macaddr8});

                // Bit String Types
                yield return new("bit", true, "1");
                yield return new("bit[]", new []{true, false}, "{1,0}");
                var bit2 = new BitArray(2) { [0] = true };
                yield return new("bit(2)", bit2);
                yield return new("bit(2)[]", new []{bit2, bit2});
                var varbit4 = new BitArray(2) { [0] = true };
                yield return new("bit varying(4)", varbit4);
                yield return new("bit varying(4)[]", new []{varbit4, varbit4});

                // Text Search Types
                var tsvector = NpgsqlTsVector.Parse("fat cat");
                yield return new("tsvector", tsvector);
                yield return new("tsvector[]", new []{tsvector, tsvector}, "{\"'cat' 'fat'\",\"'cat' 'fat'\"}");
                var tsquery = NpgsqlTsQuery.Parse("fat & (rat | cat)");
                yield return new("tsquery", tsquery, testCaseName: $"{nameof(GetFieldValue)}<{nameof(NpgsqlTsQuery)}>({{0}})");
                yield return new("tsquery[]", new []{tsquery, tsquery}, "{\"'fat' & ( 'rat' | 'cat' )\",\"'fat' & ( 'rat' | 'cat' )\"}");

                // UUID Type
                var uuid = Guid.NewGuid();
                yield return new("uuid", uuid);
                yield return new("uuid[]", new []{uuid, uuid});

                // XML Type
                var xml = "<foo>bar</foo>";
                yield return new("xml", xml);
                yield return new("xml[]", new []{xml, xml});

                // JSON Types
                var json = "{\"foo\": [true, \"bar\"], \"tags\": {\"a\": 1, \"b\": null}}";
                yield return new("json", json);
                yield return new("json[]", new []{json, json},
                    "{\"{\\\"foo\\\": [true, \\\"bar\\\"], \\\"tags\\\": {\\\"a\\\": 1, \\\"b\\\": null}}\",\"{\\\"foo\\\": [true, \\\"bar\\\"], \\\"tags\\\": {\\\"a\\\": 1, \\\"b\\\": null}}\"}");
                var jsonb = "{\"foo\": [true, \"bar\"], \"tags\": {\"a\": 1, \"b\": null}}";
                yield return new("jsonb", jsonb);
                yield return new("jsonb[]", new []{jsonb, jsonb},
                    "{\"{\\\"foo\\\": [true, \\\"bar\\\"], \\\"tags\\\": {\\\"a\\\": 1, \\\"b\\\": null}}\",\"{\\\"foo\\\": [true, \\\"bar\\\"], \\\"tags\\\": {\\\"a\\\": 1, \\\"b\\\": null}}\"}");
                var jsonpath = "$.\"id\"?(@ == 42)";
                yield return new("jsonpath", jsonpath);
                yield return new("jsonpath[]", new []{jsonpath, jsonpath}, "{\"$.\\\"id\\\"?(@ == 42)\",\"$.\\\"id\\\"?(@ == 42)\"}");

                // Composite Types
                var composite = new RDRTGFVComposite(42, "Answer to the Ultimate Question of Life, the Universe, and Everything");
                yield return new(nameof(RDRTGFVComposite), composite);
                yield return new($"{nameof(RDRTGFVComposite)}[]", new[] { composite, composite },
                    "{\"(42,\\\"Answer to the Ultimate Question of Life, the Universe, and Everything\\\")\",\"(42,\\\"Answer to the Ultimate Question of Life, the Universe, and Everything\\\")\"}"
                    , testCaseName: null);

                // Range Types
                // Domain Types
                // Object Identifier Types
                // pg_lsn Type
                // Pseudo-Types
            }
        }

        class GetFieldValueTestCase
        {
            readonly string? _stringValue;
            readonly string? _inputString;
            public string TypeName { get; }
            public object BinaryValue { get; }
            public string? ExplicitReason { get; }
            public string? IgnoreReason { get; }
            public string TestCaseName { get; } = "{m}{a}";

            public string StringValue => _stringValue
                                         ?? (BinaryValue is Array a
                                             ? $"{{{string.Join(',', a.Cast<object>().Select(Format))}}}"
                                             :  Format(BinaryValue));
            public string InputString => _inputString
                                         ?? $"$${StringValue}$$::{TypeName}";

            static string Format(object o)
                => o switch
                {
                    DateTime { Hour: 0, Minute: 0, Second: 0, Millisecond: 0 } date => date.ToString("yyyy-MM-dd"),
                    DateTime { Hour: >0, Kind: DateTimeKind.Unspecified } ts => ts.ToString("yyyy-MM-dd HH:mm:ss"),
                    DateTime { Hour: >0, Kind: DateTimeKind.Local } tstz => tstz.ToString("yyyy-MM-dd HH:mm:sszz"),
                    DateTimeOffset timetz => timetz.ToString("HH:mm:sszz"),
                    TimeSpan { Days: 0 } time => time.ToString("hh\\:mm\\:ss"),
                    TimeSpan interval => interval.ToString("%d' days 'hh\\:mm\\:ss"),
                    ValueTuple<IPAddress, int> cidr => string.Format(CultureInfo.InvariantCulture, "{0}/{1}", cidr.Item1, cidr.Item2),
                    PhysicalAddress macaddr => macaddr.GetAddressBytes()
                        switch
                        {
                            {Length: 6} m6 => string.Join(':', m6.Select(e => e.ToString("x2"))),
                            {Length: 8} m8 => string.Join(':', m8.Select(e => e.ToString("x2"))),
                            _ => throw new Exception()
                        },
                    BitArray bit => string.Concat(bit.Cast<bool>().Select(e => e ? '1' : '0')),
                    RDRTGFVComposite composite => $"({composite.Id},\"{composite.Value}\")",
                    _ => string.Format(CultureInfo.InvariantCulture, "{0}", o),
                };

            public GetFieldValueTestCase(string typeName, object binaryValue, string? stringValue = null, string? inputString = null, string? testCaseName = "{m}({0})", string? explicitReason = null, string? ignoreReason = null)
            {
                _stringValue = stringValue;
                _inputString = inputString;
                TypeName = typeName;
                BinaryValue = binaryValue;
                ExplicitReason = explicitReason;
                IgnoreReason = ignoreReason;
                if (testCaseName is not null)
                    TestCaseName = testCaseName;
            }
        }

        enum RDRTGFVEnum { Sad, Ok, Happy }

        record RDRTGFVComposite(int Id, string Value);

        #endregion

        [TestCaseSource(nameof(GetFieldValueTestCases)), NonParallelizable]
        public Task GetFieldValue<T>(string typeName, T binaryValue, string stringValue, string inputString)
            => SafePgOutputReplicationTest(
                async (slotName, tableName, publicationName) =>
                {
                    if (IsBinaryMode && typeof(T) != typeof(byte[]) && (
                        typeof(T).IsArray ||
                        typeof(T).IsEnum) ||
                        typeof(T) == typeof(RDRTGFVComposite))
                        Assert.Ignore("Type loading in logical replication connections is blocked by #3294");

                    var adjustedConnectionStringBuilder = new NpgsqlConnectionStringBuilder(ConnectionString)
                    {
                        Options = "-c lc_monetary=C -c bytea_output=escape"
                    };

                    await using var c = await OpenConnectionAsync(adjustedConnectionStringBuilder);
                    if (typeof(T) == typeof(RDRTGFVEnum) || typeof(T).IsArray && typeof(T).GetElementType()! == typeof(RDRTGFVEnum))
                    {
                        await c.ExecuteNonQueryAsync(@$"DROP TYPE IF EXISTS {nameof(RDRTGFVEnum)} CASCADE;
                                                        CREATE TYPE {nameof(RDRTGFVEnum)}
                                                        AS ENUM (
                                                            '{nameof(RDRTGFVEnum.Sad)}',
                                                            '{nameof(RDRTGFVEnum.Ok)}',
                                                            '{nameof(RDRTGFVEnum.Happy)}')");
                        NpgsqlConnection.GlobalTypeMapper.MapEnum<RDRTGFVEnum>();
                    }
                    else if (typeof(T) == typeof(RDRTGFVComposite) || typeof(T).IsArray && typeof(T).GetElementType()! == typeof(RDRTGFVComposite))
                    {
                        await c.ExecuteNonQueryAsync(@$"DROP TYPE IF EXISTS {nameof(RDRTGFVComposite)} CASCADE;
                                                        CREATE TYPE {nameof(RDRTGFVComposite)} AS (id int, value text)");
                        NpgsqlConnection.GlobalTypeMapper.MapComposite<RDRTGFVComposite>();
                    }

                    await c.ExecuteNonQueryAsync(@$"CREATE TABLE {tableName} (value {typeName});
                                                    CREATE PUBLICATION {publicationName} FOR TABLE {tableName};");
                    var rc = await OpenReplicationConnectionAsync(adjustedConnectionStringBuilder);
                    var slot = await rc.CreatePgOutputReplicationSlot(slotName);
                    await c.ExecuteNonQueryAsync(@$"INSERT INTO {tableName} VALUES ({inputString})");

                    using var streamingCts = new CancellationTokenSource();
                    var messages = SkipEmptyTransactions(rc.StartReplication(slot, GetOptions(publicationName), streamingCts.Token))
                        .GetAsyncEnumerator(streamingCts.Token);

                    // Begin Transaction, Type Relation
                    await AssertTransactionStart(messages);
                    if (typeof(T) == typeof(RDRTGFVEnum)
                        || typeof(T) == typeof(RDRTGFVComposite)
                        || typeof(T).IsArray && typeof(T).GetElementType()! == typeof(RDRTGFVEnum)
                        || typeof(T).IsArray && typeof(T).GetElementType()! == typeof(RDRTGFVComposite))
                        await NextMessage<TypeMessage>(messages);
                    await NextMessage<RelationMessage>(messages);

                    // Insert first value
                    var insertMsg = await NextMessageBuffered<InsertMessage>(messages);
                    await AssertFieldValue(insertMsg, 0, binaryValue, stringValue);

                    // Commit Transaction
                    await AssertTransactionCommit(messages);
                    streamingCts.Cancel();
                    await AssertReplicationCancellation(messages);
                    await rc.DropReplicationSlot(slotName, cancellationToken: CancellationToken.None);

                    if (typeof(T) == typeof(RDRTGFVEnum) || typeof(T).IsArray && typeof(T).GetElementType()! == typeof(RDRTGFVEnum))
                    {
                        NpgsqlConnection.GlobalTypeMapper.UnmapEnum<RDRTGFVEnum>();
                        await c.ExecuteNonQueryAsync(@$"DROP TYPE IF EXISTS {nameof(RDRTGFVEnum)} CASCADE");
                    }
                    else if (typeof(T) == typeof(RDRTGFVComposite) || typeof(T).IsArray && typeof(T).GetElementType()! == typeof(RDRTGFVComposite))
                    {
                        NpgsqlConnection.GlobalTypeMapper.UnmapComposite<RDRTGFVComposite>();
                        await c.ExecuteNonQueryAsync(@$"DROP TYPE IF EXISTS {nameof(RDRTGFVComposite)} CASCADE");
                    }


                    async Task AssertFieldValue(InsertMessage message, int ordinal, T expectedValue, string expectedStringValue)
                    {
                        if (IsBinaryMode)
                        {
                            if (Async)
                                // ReSharper disable once MethodSupportsCancellation
                                Assert.That(await message.NewRow.GetFieldValueAsync<T>(ordinal), Is.EqualTo(expectedValue));
                            else
                                Assert.That(message.NewRow.GetFieldValue<T>(ordinal), Is.EqualTo(expectedValue));
                        }
                        else
                        {
                            if (Async)
                                // ReSharper disable once MethodSupportsCancellation
                                Assert.That(await message.NewRow.GetFieldValueAsync<string>(ordinal), Is.EqualTo(expectedStringValue));
                            else
                                Assert.That(message.NewRow.GetFieldValue<string>(ordinal), Is.EqualTo(expectedStringValue));
                        }
                    }
                });

        async Task<T> NextMessageBuffered<T>(IAsyncEnumerator<PgOutputReplicationMessage> messages)
            where T : PgOutputReplicationMessage
        {
            var message = await NextMessage<T>(messages);
            if (message is InsertMessage insertMessage)
                return Buffered
                    ? Async
                        ? (T)(PgOutputReplicationMessage)await insertMessage.CloneAsync()
                        // ReSharper disable once MethodHasAsyncOverload
                        : (T)(PgOutputReplicationMessage)insertMessage.Clone()
                    : (T)(PgOutputReplicationMessage)insertMessage;

            return Buffered ? (T)message.Clone() : message;
        }

        public ReplicationDataRecordTests(ReplicationDataMode dataMode, Buffering buffering, AsyncMode asyncMode)
            : base(ProtocolVersionMode.ProtocolV1, dataMode, TransactionStreamingMode.DefaultTransaction)
        {
            Buffered = buffering == Buffering.Buffered;
            Async = asyncMode == AsyncMode.Async;
        }

        bool Buffered { get; }
        bool Async { get; }

        public enum Buffering
        {
            Unbuffered,
            Buffered
        }

        public enum AsyncMode
        {
            Sync,
            Async
        }
    }
}
