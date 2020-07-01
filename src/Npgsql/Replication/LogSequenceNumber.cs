using System;
using System.Diagnostics;
using System.Globalization;

#pragma warning disable 1591

namespace Npgsql.Replication
{
    public readonly struct LogSequenceNumber : IEquatable<LogSequenceNumber>, IEquatable<ulong>, IComparable<LogSequenceNumber>, IComparable<ulong>
    {
        readonly ulong _value;

        public LogSequenceNumber(ulong value) => _value = value;

        public bool Equals(LogSequenceNumber other) => _value.Equals(other._value);

        public bool Equals(ulong other) => _value.Equals(other);

        public int CompareTo(LogSequenceNumber other) => _value.CompareTo(other._value);

        public int CompareTo(ulong other) => _value.CompareTo(other);

        public override bool Equals(object? obj) => _value.Equals(obj);

        public override int GetHashCode() => _value.GetHashCode();

        public override string ToString() => unchecked($"{(uint)(_value >> 32):X}/{(uint)_value:X}");

        public static LogSequenceNumber Parse(string s) => Parse(s.AsSpan());

        public static LogSequenceNumber Parse(ReadOnlySpan<char> s)
        {
            for (var i = 0; i < s.Length; i++)
            {
                if (s[i] == '/')
                {

#if NET461 || NETSTANDARD2_0
                    var firstPart = s.Slice(0, i).ToString();
                    var secondPart = s.Slice(++i).ToString();
#else
                    var firstPart = s.Slice(0, i);
                    var secondPart = s.Slice(++i);
#endif
                    return new LogSequenceNumber(((ulong)uint.Parse(firstPart, NumberStyles.AllowHexSpecifier) << 32) + uint.Parse(secondPart, NumberStyles.AllowHexSpecifier));
                }
            }
            throw new FormatException($"Invalid LSN: '{s.ToString()}'.");
        }

        public static bool TryParse(string s, out LogSequenceNumber lsn) => TryParse(s.AsSpan(), out lsn);

        public static bool TryParse(ReadOnlySpan<char> s, out LogSequenceNumber lsn)
        {
            for (var i = 0; i < s.Length; i++)
            {
                if (s[i] != '/') continue;

#if NET461 || NETSTANDARD2_0
                var firstPart = s.Slice(0, i).ToString();
                var secondPart = s.Slice(++i).ToString();
#else
                    var firstPart = s.Slice(0, i);
                    var secondPart = s.Slice(++i);
#endif

                if (!uint.TryParse(firstPart, NumberStyles.AllowHexSpecifier, null, out var first))
                {
                    lsn = default;
                    return false;
                }
                if (!uint.TryParse(secondPart, NumberStyles.AllowHexSpecifier, null, out var second))
                {
                    lsn = default;
                    return false;
                }
                lsn = new LogSequenceNumber(((ulong)first << 32) + second);
                return true;
            }
            lsn = default;
            return false;
        }

        public static implicit operator LogSequenceNumber(ulong val) => new LogSequenceNumber(val);

        public static implicit operator ulong(LogSequenceNumber val) => val._value;

        public static bool operator ==(LogSequenceNumber val1, LogSequenceNumber val2) => val1._value.Equals(val2._value);

        public static bool operator !=(LogSequenceNumber val1, LogSequenceNumber val2) => !val1._value.Equals(val2._value);

        public static bool operator >(LogSequenceNumber val1, LogSequenceNumber val2) => val1._value > val2._value;

        public static bool operator <(LogSequenceNumber val1, LogSequenceNumber val2) => val1._value < val2._value;

        public static bool operator >=(LogSequenceNumber val1, LogSequenceNumber val2) => val1._value >= val2._value;

        public static bool operator <=(LogSequenceNumber val1, LogSequenceNumber val2) => val1._value <= val2._value;
    }
}
