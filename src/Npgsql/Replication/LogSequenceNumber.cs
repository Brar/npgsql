using System;
using System.Globalization;
using JetBrains.Annotations;

namespace Npgsql.Replication
{
    /// <summary>
    /// Wraps a PostgreSQL Write-Ahead Log Sequence Number (LSN, XLogRecPtr) providing conversions
    /// from/to <see cref="string"/> and <see cref="ulong"/> as well as some convenience methods and operators
    /// </summary>
    /// <remarks>
    /// Log Sequence Numbers are a fundamental concept of the PostgreSQL Write-Ahead Log and by that of
    /// PostgreSQL replication. See https://www.postgresql.org/docs/current/wal-internals.html for what they represent.
    /// </remarks>
    public readonly struct LogSequenceNumber
        : IEquatable<LogSequenceNumber>, IEquatable<ulong>, IComparable<LogSequenceNumber>, IComparable<ulong>
    {
        /// <summary>
        /// Zero is used indicate an invalid Log Sequence Number. No XLOG record can begin at zero.
        /// </summary>
        public static readonly LogSequenceNumber Invalid = new LogSequenceNumber(0ul);

        readonly ulong _value;

        /// <summary>
        /// Initializes a new instance of <see cref="LogSequenceNumber"/>.
        /// </summary>
        /// <param name="value">The value to wrap.</param>
        [PublicAPI]
        public LogSequenceNumber(ulong value)
            => _value = value;

        /// <summary>
        /// Returns a value indicating whether this instance is equal to a specified <see cref="LogSequenceNumber"/>
        /// instance.
        /// </summary>
        /// <param name="other">A <see cref="LogSequenceNumber"/> instance to compare to this instance.</param>
        /// <returns><c>true</c> if the current instance is equal to the value parameter;
        /// otherwise, <c>false</c>.</returns>
        [PublicAPI]
        public bool Equals(LogSequenceNumber other)
            => _value.Equals(other._value);

        /// <summary>
        /// Returns a value indicating whether this instance is equal to a specified <see cref="ulong"/> value.
        /// </summary>
        /// <param name="value">An <see cref="ulong"/> value to compare to this instance.</param>
        /// <returns><c>true</c> if the current instance is equal to the value parameter;
        /// otherwise, <c>false</c>.</returns>
        [PublicAPI]
        public bool Equals(ulong value)
            => _value.Equals(value);

        /// <summary>
        /// Compares this instance to a specified <see cref="LogSequenceNumber"/> and returns an indication of their
        /// relative values.
        /// </summary>
        /// <param name="value">A <see cref="LogSequenceNumber"/> instance to compare to this instance.</param>
        /// <returns>A signed number indicating the relative values of this instance and <c>value</c>.</returns>
        [PublicAPI]
        public int CompareTo(LogSequenceNumber value)
            => _value.CompareTo(value._value);

        /// <summary>
        /// Compares this instance to a specified <see cref="long"/> value and returns an indication of their relative
        /// values.
        /// </summary>
        /// <param name="value">A <see cref="long"/> value to compare to this instance.</param>
        /// <returns>A signed number indicating the relative values of this instance and <c>value</c>.</returns>
        [PublicAPI]
        public int CompareTo(ulong value)
            => _value.CompareTo(value);

        /// <summary>
        /// Returns a value indicating whether this instance is equal to a specified object.
        /// </summary>
        /// <param name="obj">An object to compare to this instance</param>
        /// <returns><c>true</c> if the current instance is equal to the value parameter;
        /// otherwise, <c>false</c>.</returns>
        [PublicAPI]
        public override bool Equals(object? obj)
            => _value.Equals(obj);

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>A 32-bit signed integer hash code.</returns>
        [PublicAPI]
        public override int GetHashCode()
            => _value.GetHashCode();

        /// <summary>
        /// Converts the numeric value of this instance to its equivalent string representation.
        /// </summary>
        /// <returns>The string representation of the value of this instance, consisting of two hexadecimal numbers of
        /// up to 8 digits each, separated by a slash</returns>
        [PublicAPI]
        public override string ToString()
            => unchecked($"{(uint)(_value >> 32):X}/{(uint)_value:X}");

        /// <summary>
        /// Converts the string representation of a Log Sequence Number to a <see cref="LogSequenceNumber"/> instance.
        /// </summary>
        /// <param name="s">A string that represents the Log Sequence Number to convert.</param>
        /// <returns>
        /// A <see cref="LogSequenceNumber"/> equivalent to the Log Sequence Number specified in <c>s</c>.
        /// </returns>
        /// <exception cref="ArgumentNullException">The <c>s</c> parameter is <see langword="null"/>.</exception>
        /// <exception cref="OverflowException">
        /// The <c>s</c> parameter represents a number less than <see cref="ulong.MinValue"/> or greater than
        /// <see cref="ulong.MaxValue"/>.
        /// </exception>
        /// <exception cref="FormatException">The <c>s</c> parameter is not in the right format.</exception>
        [PublicAPI]
        public static LogSequenceNumber Parse(string s)
            => s == null
                ? throw new ArgumentNullException(nameof(s))
                : Parse(s.AsSpan());

        /// <summary>
        /// Converts the span representation of a Log Sequence Number to a <see cref="LogSequenceNumber"/> instance.
        /// </summary>
        /// <param name="s">A span containing the characters that represent the Log Sequence Number to convert.</param>
        /// <returns>
        /// A <see cref="LogSequenceNumber"/> equivalent to the Log Sequence Number specified in <c>s</c>.
        /// </returns>
        /// <exception cref="OverflowException">
        /// The <c>s</c> parameter represents a number less than <see cref="ulong.MinValue"/> or greater than
        /// <see cref="ulong.MaxValue"/>.
        /// </exception>
        /// <exception cref="FormatException">The <c>s</c> parameter is not in the right format.</exception>
        [PublicAPI]
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
            throw new FormatException($"Invalid Log Sequence Number: '{s.ToString()}'.");
        }

        /// <summary>
        /// Tries to convert the string representation of a Log Sequence Number to a <see cref="LogSequenceNumber"/>
        /// instance. A return value indicates whether the conversion succeeded or failed.
        /// </summary>
        /// <param name="s">A string that represents the Log Sequence Number to convert.</param>
        /// <param name="result">
        /// When this method returns, contains a <see cref="LogSequenceNumber"/> instance equivalent to the Log Sequence
        /// Number contained in s, if the conversion succeeded, or the default value for
        /// <see cref="LogSequenceNumber"/> (<c>0</c>) if the conversion failed. The conversion fails if the <c>s</c>
        /// parameter is <see langword="null"/> or <see cref="string.Empty"/>, is not in the right format, or represents a number
        /// less than <see cref="ulong.MinValue"/> or greater than <see cref="ulong.MaxValue"/>. This parameter is
        /// passed uninitialized; any value originally supplied in result will be overwritten.
        /// </param>
        /// <returns><c>true</c> if <c>s</c> was converted successfully; otherwise, <c>false</c>.</returns>
        [PublicAPI]
        public static bool TryParse(string s, out LogSequenceNumber result)
        {
            if (s != null)
                return TryParse(s.AsSpan(), out result);

            result = default;
            return false;
        }

        /// <summary>
        /// Tries to convert the span representation of a Log Sequence Number to a <see cref="LogSequenceNumber"/>
        /// instance. A return value indicates whether the conversion succeeded or failed.
        /// </summary>
        /// <param name="s">A span containing the characters that represent the Log Sequence Number to convert.</param>
        /// <param name="result">
        /// When this method returns, contains a <see cref="LogSequenceNumber"/> instance equivalent to the Log Sequence
        /// Number contained in s, if the conversion succeeded, or the default value for
        /// <see cref="LogSequenceNumber"/> (<c>0</c>) if the conversion failed. The conversion fails if the <c>s</c>
        /// parameter is empty, is not in the right format, or represents a number less than
        /// <see cref="ulong.MinValue"/> or greater than <see cref="ulong.MaxValue"/>. This parameter is passed
        /// uninitialized; any value originally supplied in result will be overwritten.
        /// </param>
        /// <returns><c>true</c> if <c>s</c> was converted successfully; otherwise, <c>false</c>.</returns>
        [PublicAPI]
        public static bool TryParse(ReadOnlySpan<char> s, out LogSequenceNumber result)
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
                    result = default;
                    return false;
                }
                if (!uint.TryParse(secondPart, NumberStyles.AllowHexSpecifier, null, out var second))
                {
                    result = default;
                    return false;
                }
                result = new LogSequenceNumber(((ulong)first << 32) + second);
                return true;
            }
            result = default;
            return false;
        }

        /// <summary>
        /// Converts the value of a 64-bit unsigned integer to a <see cref="LogSequenceNumber"/> instance.
        /// </summary>
        /// <param name="value">A 64-bit unsigned integer.</param>
        /// <returns>A new instance of <see cref="LogSequenceNumber"/> initialized to <c>value</c>.</returns>
        [PublicAPI]
        public static implicit operator LogSequenceNumber(ulong value)
            => new LogSequenceNumber(value);

        /// <summary>
        /// Converts the value of a <see cref="LogSequenceNumber"/> instance to a 64-bit unsigned integer value.
        /// </summary>
        /// <param name="value">A <see cref="LogSequenceNumber"/> instance</param>
        /// <returns>The contents of <c>value</c> as 64-bit unsigned integer.</returns>
        [PublicAPI]
        public static implicit operator ulong(LogSequenceNumber value)
            => value._value;

        /// <summary>
        /// Returns a value that indicates whether two specified instances of <see cref="LogSequenceNumber"/> are equal.
        /// </summary>
        /// <param name="value1">The first Log Sequence Number to compare.</param>
        /// <param name="value2">The second Log Sequence Number to compare.</param>
        /// <returns><c>true</c> if <c>value1</c> equals <c>value2</c>; otherwise, <c>false</c>.</returns>
        [PublicAPI]
        public static bool operator ==(LogSequenceNumber value1, LogSequenceNumber value2)
            => value1._value.Equals(value2._value);

        /// <summary>
        /// Returns a value that indicates whether two specified instances of <see cref="LogSequenceNumber"/> are not
        /// equal.
        /// </summary>
        /// <param name="value1">The first Log Sequence Number to compare.</param>
        /// <param name="value2">The second Log Sequence Number to compare.</param>
        /// <returns><c>true</c> if <c>value1</c> does not equal <c>value2</c>; otherwise, <c>false</c>.</returns>
        [PublicAPI]
        public static bool operator !=(LogSequenceNumber value1, LogSequenceNumber value2)
            => !value1._value.Equals(value2._value);

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="LogSequenceNumber"/> instance is greater than
        /// another specified <see cref="LogSequenceNumber"/> instance.
        /// </summary>
        /// <param name="value1">The first value to compare.</param>
        /// <param name="value2">The second value to compare.</param>
        /// <returns><c>true</c> if <c>value1</c> is greater than <c>value2</c>; otherwise, <c>false</c>.</returns>
        [PublicAPI]
        public static bool operator >(LogSequenceNumber value1, LogSequenceNumber value2)
            => value1._value > value2._value;

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="LogSequenceNumber"/> instance is less than
        /// another specified <see cref="LogSequenceNumber"/> instance.
        /// </summary>
        /// <param name="value1">The first value to compare.</param>
        /// <param name="value2">The second value to compare.</param>
        /// <returns><c>true</c> if <c>value1</c> is less than <c>value2</c>; otherwise, <c>false</c>.</returns>
        [PublicAPI]
        public static bool operator <(LogSequenceNumber value1, LogSequenceNumber value2)
            => value1._value < value2._value;

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="LogSequenceNumber"/> instance is greater than or
        /// equal to another specified <see cref="LogSequenceNumber"/> instance.
        /// </summary>
        /// <param name="value1">The first value to compare.</param>
        /// <param name="value2">The second value to compare.</param>
        /// <returns>
        /// <c>true</c> if <c>value1</c> is greater than or equal to <c>value2</c>; otherwise, <c>false</c>.
        /// </returns>
        [PublicAPI]
        public static bool operator >=(LogSequenceNumber value1, LogSequenceNumber value2)
            => value1._value >= value2._value;

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="LogSequenceNumber"/> instance is less than or
        /// equal to another specified <see cref="LogSequenceNumber"/> instance.
        /// </summary>
        /// <param name="value1">The first value to compare.</param>
        /// <param name="value2">The second value to compare.</param>
        /// <returns>
        /// <c>true</c> if <c>value1</c> is less than or equal to <c>value2</c>; otherwise, <c>false</c>.
        /// </returns>
        [PublicAPI]
        public static bool operator <=(LogSequenceNumber value1, LogSequenceNumber value2)
            => value1._value <= value2._value;

        /// <summary>
        /// Subtracts two specified <see cref="LogSequenceNumber"/> values.
        /// </summary>
        /// <param name="first">The first <see cref="LogSequenceNumber"/> value.</param>
        /// <param name="second">The second <see cref="LogSequenceNumber"/> value.</param>
        /// <returns>The number of bytes separating those write-ahead log locations.</returns>
        [PublicAPI]
        public static ulong operator -(LogSequenceNumber first, LogSequenceNumber second)
            => new LogSequenceNumber(
                first._value > second._value
                    ? first._value-second._value
                    : second._value-first._value);

        /// <summary>
        /// Subtract the number of bytes from a <see cref="LogSequenceNumber"/> instance, giving a new
        /// <see cref="LogSequenceNumber"/> instance.
        /// </summary>
        /// <param name="lsn">
        /// The <see cref="LogSequenceNumber"/> instance representing a write-ahead log location.
        /// </param>
        /// <param name="nbytes">The number of bytes to subtract.</param>
        /// <returns>A new <see cref="LogSequenceNumber"/> instance.</returns>
        /// <exception cref="OverflowException">
        /// The resulting <see cref="LogSequenceNumber"/> instance would represent a number less than
        /// <see cref="ulong.MinValue"/>.
        /// </exception>
        [PublicAPI]
        public static LogSequenceNumber operator -(LogSequenceNumber lsn, ulong nbytes)
            => new LogSequenceNumber(checked(lsn._value - nbytes));

        /// <summary>
        /// Add the number of bytes to a <see cref="LogSequenceNumber"/> instance, giving a new
        /// <see cref="LogSequenceNumber"/> instance.
        /// </summary>
        /// <param name="lsn">
        /// The <see cref="LogSequenceNumber"/> instance representing a write-ahead log location.
        /// </param>
        /// <param name="nbytes">The number of bytes to add.</param>
        /// <returns>A new <see cref="LogSequenceNumber"/> instance.</returns>
        /// <exception cref="OverflowException">
        /// The resulting <see cref="LogSequenceNumber"/> instance would represent a number greater than
        /// <see cref="ulong.MaxValue"/>.
        /// </exception>
        [PublicAPI]
        public static LogSequenceNumber operator +(LogSequenceNumber lsn, ulong nbytes)
            => new LogSequenceNumber(checked(lsn._value + nbytes));
    }
}
