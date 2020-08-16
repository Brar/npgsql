using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NpgsqlTypes;
using NUnit.Framework;

namespace Npgsql.Tests.Types
{
    public class TimestampTests : MultiplexingTestBase
    {
        public TimestampTests(MultiplexingMode multiplexingMode) : base(multiplexingMode) {}

        [Test]
        [TestCase("-infinity", ExpectedResult = long.MinValue)]
        [TestCase("4714-11-24 00:00:00 BC", ExpectedResult = -211813488000000000L)]
        [TestCase("2000-01-01 00:00:00", ExpectedResult = 0L)]
        [TestCase("294276-12-31 23:59:59.999999", ExpectedResult = 9223371331199999999L)]
        [TestCase("epoch", ExpectedResult = -946684800000000L)]
        [TestCase("infinity", ExpectedResult = long.MaxValue)]
        public async Task<long> ReadRaw(string value)
        {
            await using var conn = await OpenConnectionAsync();
            using var command = new NpgsqlCommand($"SELECT @p::timestamp", conn);
            command.Parameters.Add(new NpgsqlParameter<string>("p", value));
            await using var reader = await command.ExecuteReaderAsync();
            Assert.That(await reader.ReadAsync(), Is.True);
            return reader.GetFieldValue<Timestamp>(0).RawValue;
        }

        [Test]
        [TestCase(long.MinValue, ExpectedResult = "-infinity")]
        [TestCase(-211813488000000000L, ExpectedResult = "4714-11-24 00:00:00 BC")]
        [TestCase(0L, ExpectedResult = "2000-01-01 00:00:00")]
        [TestCase(9223371331199999999L, ExpectedResult = "294276-12-31 23:59:59.999999")]
        [TestCase(-946684800000000L, ExpectedResult = "1970-01-01 00:00:00")]
        [TestCase(long.MaxValue, ExpectedResult = "infinity")]
        public async Task<string> WriteRaw(long value)
        {
            await using var conn = await OpenConnectionAsync();
            using var command = new NpgsqlCommand($"SELECT @p::text", conn);
            command.Parameters.Add(new NpgsqlParameter<Timestamp>("p", new Timestamp(value)));
            await using var reader = await command.ExecuteReaderAsync();
            Assert.That(await reader.ReadAsync(), Is.True);
            return reader.GetFieldValue<string>(0);
        }

        static readonly TestCaseData[] FromDateTimeCases = {
            new TestCaseData(new DateTime(637331867999999995L), false)
                .SetArgDisplayNames(new DateTime(637331867999999995L).ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF"), "false")
                .SetDescription($"Tests whether {nameof(Timestamp.FromDateTime)} returns the correct truncated value, given a {nameof(DateTime)} value utilizing 100 nanosecond precision when roundToMicroseconds is set to false.")
                .Returns(new Timestamp(650905199999999L)),
            new TestCaseData(new DateTime(637331867999999994L), false)
                .SetArgDisplayNames(new DateTime(637331867999999994L).ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF"), "false")
                .SetDescription($"Tests whether {nameof(Timestamp.FromDateTime)} returns the correct truncated value, given a {nameof(DateTime)} value utilizing 100 nanosecond precision when roundToMicroseconds is set to false.")
                .Returns(new Timestamp(650905199999999L)),
            new TestCaseData(new DateTime(637331867999999995L), true)
                .SetArgDisplayNames(new DateTime(637331867999999995L).ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF"), "true")
                .SetDescription($"Tests whether {nameof(Timestamp.FromDateTime)} returns the correct rounded value, given a {nameof(DateTime)} value utilizing 100 nanosecond precision when roundToMicroseconds is set to true.")
                .Returns(new Timestamp(650905200000000L)),
            new TestCaseData(new DateTime(637331867999999994L), true)
                .SetArgDisplayNames(new DateTime(637331867999999994L).ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF"), "true")
                .SetDescription($"Tests whether {nameof(Timestamp.FromDateTime)} returns the correct rounded value, given a {nameof(DateTime)} value utilizing 100 nanosecond precision when roundToMicroseconds is set to true.")
                .Returns(new Timestamp(650905199999999L)),
        };

        [Test, TestCaseSource(nameof(FromDateTimeCases))]
        public Timestamp FromDateTime(DateTime value, bool roundToMicroseconds)
            => Timestamp.FromDateTime(value, roundToMicroseconds);

        static readonly TestCaseData[] ToStringCases = {
            new TestCaseData(Timestamp.NegativeInfinity).Returns("-infinity"),
            new TestCaseData(Timestamp.MinValue).Returns("4714-11-24 00:00:00 BC"),
            new TestCaseData(Timestamp.Epoch).Returns("1970-01-01 00:00:00"),
            new TestCaseData(Timestamp.PostgresEpoch).Returns("2000-01-01 00:00:00"),
            new TestCaseData(new Timestamp(9223371331199999990L)).Returns("294276-12-31 23:59:59.99999"),
            new TestCaseData(new Timestamp(9223371331199999991L)).Returns("294276-12-31 23:59:59.999991"),
            new TestCaseData(new Timestamp(9223371331199099999L)).Returns("294276-12-31 23:59:59.099999"),
            new TestCaseData(Timestamp.MaxValue).Returns("294276-12-31 23:59:59.999999"),
            new TestCaseData(Timestamp.Infinity).Returns("infinity"),
        };

        [Test, TestCaseSource(nameof(ToStringCases))]
        public string ToString(Timestamp value)
            => value.ToString();
    }
}
