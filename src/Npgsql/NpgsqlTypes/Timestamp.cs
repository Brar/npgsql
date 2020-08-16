#if !LegacyProviderSpecificDateTimeTypes
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Npgsql;
using Npgsql.TypeHandlers.DateTimeHandlers;

// ReSharper disable once CheckNamespace
namespace NpgsqlTypes
{
    /// <summary>
    /// A .NET implementation of PostgreSQL's timestamp type.
    /// </summary>
    /// <remarks>
    /// The implementation closely follows PostgreSQL's timestamp type
    /// (see https://www.postgresql.org/docs/current/datatype-datetime.html
    /// for details) and is only useful if, for some reason, you need the
    /// full year range and more limited precision of that type.
    /// In all but very few cases you are probably better off using the
    /// Instant type from NodaTime
    /// (see https://nodatime.org/3.0.x/api/NodaTime.Instant.html) together
    /// with the Npgsql NodaTime Type Plugin
    /// (see http://www.npgsql.org/doc/types/nodatime.html) or just
    /// the plain .NET <see cref="DateTime"/> type.
    /// </remarks>
    [Serializable]
    public readonly struct Timestamp : IEquatable<Timestamp>, IEqualityComparer<Timestamp>, IEqualityComparer,
        IComparable<Timestamp>, IComparable, IComparer<Timestamp>, IComparer
    {
        #region Constants

        const int MaxTimestampPrecision = 6;

        const int MonthsPerYear = 12;

        const int SecsPerDay = 86400;
        const int SecsPerHour = 3600;
        const int SecsPerMinute = 60;
        const int MinsPerHour = 60;

        const long MicroSecsPerDay = 86400000000L;
        const long  MicroSecsPerHour = 3600000000L;
        const long  MicroSecsPerMinute = 60000000L;
        const long  MicroSecsPerSec = 1000000L;

        const long PostgresEpochJulianDate = 2451545L;


        const long PostgresTimestampOffsetTicks = 630822816000000000L;
        const long DateTimeMinValue = -63082281600000000L;
        const long DateTimeMaxValue = 252455615999999999L;

        /// <summary>
        /// Earlier than all other <see cref="Timestamp"/> values.
        /// </summary>
        public static readonly Timestamp NegativeInfinity = new Timestamp(long.MinValue);

        /// <summary>
        /// Represents the smallest possible value of a <see cref="Timestamp"/> which is 4714-11-24 00:00:00 BC.
        /// </summary>
        public static readonly Timestamp MinValue = new Timestamp(-211813488000000000L);

        /// <summary>
        /// Represents the Unix system time zero which is 1970-01-01 00:00:00.
        /// </summary>
        public static readonly Timestamp Epoch = new Timestamp(-946684800000000L);

        /// <summary>
        /// Represents the PostgreSQL time zero which is 2000-01-01 00:00:00.
        /// </summary>
        public static readonly Timestamp PostgresEpoch = new Timestamp(0L);

        /// <summary>
        /// Represents the largest possible value of a <see cref="Timestamp"/> which is 294276-12-31 23:59:59.999999.
        /// </summary>
        public static readonly Timestamp MaxValue = new Timestamp(9223371331199999999L);

        /// <summary>
        /// Later than all other <see cref="Timestamp"/> values.
        /// </summary>
        public static readonly Timestamp Infinity = new Timestamp(long.MaxValue);

        #endregion

        #region Static functions

        /// <summary>
        /// Converts a <see cref="DateTime"/> value to a <see cref="Timestamp"/>.
        /// </summary>
        /// <remarks>
        /// Since the <see cref="Timestamp"/> type doesn't have a concept of
        /// a time zone, the <see cref="DateTime.Kind"/> property is ignored
        /// when converting from <see cref="DateTime"/>.
        /// </remarks>
        /// <param name="value">The <see cref="DateTime"/> value to convert.</param>
        /// <param name="roundToMicroseconds">Set to <c>true</c> if <paramref name="value"/> should
        /// be rounded to full microseconds; otherwise <c>false</c> to truncate the last digit (100 ns).</param>
        /// <returns></returns>
        public static Timestamp FromDateTime(DateTime value, bool roundToMicroseconds = false)
            => new Timestamp(
                roundToMicroseconds
                    ? (long)Math.Round((value.Ticks - PostgresTimestampOffsetTicks) / 10D, MidpointRounding.AwayFromZero)
                    : (value.Ticks - PostgresTimestampOffsetTicks) / 10L);

        #endregion Static functions

        /// <summary>
        /// 
        /// </summary>
        /// <param name="convertInfinity"></param>
        /// <returns></returns>
        /// <exception cref="InvalidCastException"></exception>
        public DateTime ToDateTime(bool convertInfinity = false)
            => new DateTime(
                _value >= DateTimeMinValue && _value <= DateTimeMaxValue
                        ?  _value * 10 + PostgresTimestampOffsetTicks
                        : convertInfinity && (_value == long.MinValue || _value == long.MaxValue)
                            ? _value == long.MinValue ? DateTimeMinValue : DateTimeMaxValue
                            : throw new InvalidCastException($"The {nameof(Timestamp)} '{this}' is out of the range of DateTime ('{DateTime.MinValue:yyyy-MM-dd HH:mm:ss.FFFFFFF}' to '{DateTime.MaxValue:yyyy-MM-dd HH:mm:ss.FFFFFFF}').")
                , DateTimeKind.Unspecified);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (_value == long.MinValue)
                return "-infinity";

            if (_value == long.MaxValue)
                return "infinity";

            var result = new StringBuilder();

            var	timestamp = _value;
            var		sb = new StringBuilder();

            if (timestamp2tm(timestamp, out var tm, out var fsec))
                EncodeDateTime(tm, fsec, false, 0, null, DateStyle.UseIsoDates, DateOrder.YMD, sb);
            else
                throw new FormatException("timestamp out of range");

            return sb.ToString();
        }

        static void EncodeTime(StringBuilder str, pg_tm tm, int fsec)
        {
            str.AppendFormat("{0:D2}", tm.tm_hour);
            str.Append(':');
            str.AppendFormat("{0:D2}", tm.tm_min);
            str.Append(':');
            str.AppendFormat("{0:D2}", Math.Abs(tm.tm_sec));
            if (fsec != 0)
            {
                str.Append('.');
                var value = Math.Abs(fsec);
                var gotnonzero = false;
                var precision = MaxTimestampPrecision;
                var cp = new StringBuilder(precision, precision);


                /*
                 * Append the fractional seconds part.  Note that we don't want any
                 * trailing zeros here, so since we're building the number in reverse
                 * we'll skip appending zeros until we've output a non-zero digit.
                 */
                while (precision-- > 0)
                {
                    var oldval = value;

                    value /= 10;
                    var remainder = oldval - value * 10;

                    /* check if we got a non-zero */
                    if (remainder > 0)
                        gotnonzero = true;

                    if (gotnonzero)
                    {
                        if (cp.Length <= precision)
                            cp.Append(new string('0', precision + 1));

                        cp[precision] = (char)('0' + remainder);
                    }
                }

                /*
                 * If we still have a non-zero value then precision must have not been
                 * enough to print the number.  We fall back to a less efficient
                 * solution using TrimEnd().
                 */
                str.Append(value > 0 ? Math.Abs(fsec).ToString("D6").TrimEnd('0') : cp.ToString());
            }
        }

        void EncodeTimezone(StringBuilder str, int tz, DateStyle style)
        {
            var sec = Math.Abs(tz);
            var min = sec / SecsPerMinute;
            sec -= min * SecsPerMinute;
            var hour = min / MinsPerHour;
            min -= hour * MinsPerHour;

            /* TZ is negated compared to sign we wish to display ... */
            str.Append(tz <= 0 ? '+' : '-');

            if (sec != 0)
            {
                str.AppendFormat("{0:D2}", hour);
                str.Append(':');
                str.AppendFormat("{0:D2}", min);
                str.Append(':');
                str.AppendFormat("{0:D2}", sec);
            }
            else if (min != 0 || style == DateStyle.UseXsdDates)
            {
                str.AppendFormat("{0:D2}", hour);
                str.Append(':');
                str.AppendFormat("{0:D2}", min);
            }
            else
                str.AppendFormat("{0:D2}", hour);
        }

        // ReSharper disable once InconsistentNaming
        void EncodeDateTime(pg_tm tm, int fsec, bool print_tz, int tz, string? tzn, DateStyle style, DateOrder dateOrder, StringBuilder str)
        {
            Debug.Assert(tm.tm_mon >= 1 && tm.tm_mon <= MonthsPerYear);

	        /*
	         * Negative tm_isdst means we have no valid time zone translation.
	         */
	        //if (tm.tm_isdst < 0)
		       // print_tz = false;

	        switch (style)
	        {
		        case DateStyle.UseIsoDates:
		        case DateStyle.UseXsdDates:
			        /* Compatible with ISO-8601 date formats */
                    str.AppendFormat("{0:D4}", tm.tm_year > 0 ? tm.tm_year : -(tm.tm_year - 1));
                    str.Append('-');
                    str.AppendFormat("{0:D2}", tm.tm_mon);
                    str.Append('-');
                    str.AppendFormat("{0:D2}", tm.tm_mday);
                    str.Append(style == DateStyle.UseIsoDates ? ' ' : 'T');
                    EncodeTime(str, tm, fsec);
                    if (print_tz)
				        EncodeTimezone(str, tz, style);
			        break;

		        case DateStyle.UseSQLDates:
			        /* Compatible with Oracle/Ingres date formats */
			        if (dateOrder == DateOrder.DMY)
			        {
                        str.AppendFormat("{0:D2}", tm.tm_mday);
                        str.Append('/');
                        str.AppendFormat("{0:D2}", tm.tm_mon);
			        }
			        else
			        {
                        str.AppendFormat("{0:D2}", tm.tm_mon);
                        str.Append('/');
                        str.AppendFormat("{0:D2}", tm.tm_mday);
			        }
                    str.Append('/');
                    str.AppendFormat("{0:D4}", tm.tm_year > 0 ? tm.tm_year : -(tm.tm_year - 1));
                    str.Append(' ');
                    str.AppendFormat("{0:D2}", tm.tm_hour);
                    str.Append(':');
                    str.AppendFormat("{0:D2}", tm.tm_min);
                    str.Append(':');
                    str.AppendFormat("{0:D2}", Math.Abs(tm.tm_sec));
                    if (fsec != 0)
                    {
                        str.Append('.');
                        str.AppendFormat("{0:D6}", Math.Abs(fsec));
                    }
                    if (print_tz)
			        {
                        if (!string.IsNullOrWhiteSpace(tzn))
                            str.Append(tzn);
                        else
                            EncodeTimezone(str, tz, style);
			        }
			        break;

		        case DateStyle.UseGermanDates:
			        /* German variant on European style */
                    str.AppendFormat("{0:D2}", tm.tm_mday);
                    str.Append('.');
                    str.AppendFormat("{0:D2}", tm.tm_mon);
                    str.Append('.');
                    str.AppendFormat("{0:D4}", tm.tm_year > 0 ? tm.tm_year : -(tm.tm_year - 1));
                    str.Append(' ');
                    str.AppendFormat("{0:D2}", tm.tm_hour);
                    str.Append(':');
                    str.AppendFormat("{0:D2}", tm.tm_min);
                    str.Append(':');
                    str.AppendFormat("{0:D2}", Math.Abs(tm.tm_sec));
                    if (fsec != 0)
                    {
                        str.Append('.');
                        str.AppendFormat("{0:D6}", Math.Abs(fsec));
                    }
                    if (print_tz)
			        {
                        if (!string.IsNullOrWhiteSpace(tzn))
                            str.Append(tzn);
                        else
                            EncodeTimezone(str, tz, style);
			        }
			        break;

		        case DateStyle.UsePostgresDates:
		        default:
			        /* Backward-compatible with traditional Postgres abstime dates */
			        var day = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
			        tm.tm_wday = j2day(day);
                    str.Append(days[tm.tm_wday]);
                    str.Append(' ');
			        if (dateOrder == DateOrder.DMY)
			        {
                        str.AppendFormat("{0:D2}", tm.tm_mday);
                        str.Append(' ');
                        str.Append(months[tm.tm_mon - 1]);
			        }
			        else
			        {
                        str.Append(months[tm.tm_mon - 1]);
                        str.Append(' ');
                        str.AppendFormat("{0:D2}", tm.tm_mday);
			        }
                    str.Append(' ');
                    str.AppendFormat("{0:D2}", tm.tm_hour);
                    str.Append(':');
                    str.AppendFormat("{0:D2}", tm.tm_min);
                    str.Append(':');
                    str.AppendFormat("{0:D2}", Math.Abs(tm.tm_sec));
                    if (fsec != 0)
                    {
                        str.Append('.');
                        str.AppendFormat("{0:D6}", Math.Abs(fsec));
                    }
                    str.Append(' ');
                    str.AppendFormat("{0:D4}", tm.tm_year > 0 ? tm.tm_year : -(tm.tm_year - 1));

			        if (print_tz)
			        {
                        if (!string.IsNullOrWhiteSpace(tzn))
                            str.Append(tzn);
                        else
				        {
					        /*
					         * We have a time zone, but no string version. Use the
					         * numeric form, but be sure to include a leading space to
					         * avoid formatting something which would be rejected by
					         * the date/time parser later. - thomas 2001-10-19
					         */
                            str.Append(' ');
					        EncodeTimezone(str, tz, style);
				        }
			        }
			        break;
	        }

	        if (tm.tm_year <= 0)
                str.Append(" BC");
        }

        // ReSharper disable once InconsistentNaming
        static readonly string[] months =
            { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

        // ReSharper disable once InconsistentNaming
        static readonly string[] days =
            { "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" };


        // ReSharper disable once InconsistentNaming
        int j2day(int date)
        {
            date += 1;
            date %= 7;
            /* Cope if division truncates towards zero, as it probably does */
            if (date < 0)
                date += 7;

            return date;
        }

        // ReSharper disable once InconsistentNaming
        int date2j(int y, int m, int d)
        {
            if (m > 2)
            {
                m += 1;
                y += 4800;
            }
            else
            {
                m += 13;
                y += 4799;
            }

            var century = y / 100;
            var julian = y * 365 - 32167;
            julian += y / 4 - century + century / 4;
            julian += 7834 * m / 256 + d;

            return julian;
        }

        enum DateStyle
        {
            UsePostgresDates = 0,
            UseIsoDates = 1,
            UseSQLDates = 2,
            UseGermanDates = 3,
            UseXsdDates = 4,
        }

        enum DateOrder
        {
            // ReSharper disable InconsistentNaming
            YMD = 0,
            DMY = 1,
            MDY = 2,
            // ReSharper restore InconsistentNaming
        }

        readonly long _value;

        internal long RawValue => _value;

        internal Timestamp(long value) => _value = value;

        #region Equals

        /// <inheritdoc />
        public bool Equals(Timestamp other) => _value.Equals(other._value);

        /// <inheritdoc />
        public bool Equals(Timestamp x, Timestamp y) => x._value.Equals(y._value);

        /// <inheritdoc />
        public override bool Equals(object? obj) => obj is Timestamp timestamp && _value.Equals(timestamp._value);

        bool IEqualityComparer.Equals(object? x, object? y)
            => x is Timestamp timestampX && y is Timestamp timestampY
                ? timestampX._value.Equals(timestampY._value)
                : x == y;

        #endregion Equals

        #region GetHashCode

        /// <inheritdoc />
        public int GetHashCode(Timestamp obj) => obj._value.GetHashCode();

        /// <inheritdoc />
        public override int GetHashCode() => _value.GetHashCode();

        int IEqualityComparer.GetHashCode(object obj)
            => obj is Timestamp timestamp
                ? timestamp._value.GetHashCode()
                : obj?.GetHashCode() ?? throw new ArgumentNullException(nameof(obj));

        #endregion GetHashCode

        #region CompareTo/Compare

        /// <inheritdoc />
        public int CompareTo(Timestamp other) => _value.CompareTo(other._value);

        int IComparable.CompareTo(object? obj)
            => obj is Timestamp timestamp
                ? _value.CompareTo(timestamp._value)
                : 1;

        /// <inheritdoc />
        public int Compare(Timestamp x, Timestamp y) => x._value.CompareTo(y._value);

        int IComparer.Compare(object? x, object? y)
            => x is Timestamp timestampX && y is Timestamp timestampY
                ? timestampX._value.CompareTo(timestampY._value)
                : x == null
                    ? y == null ? 0 : -1
                    : y == null
                        ? 1
                        : Comparer.Default.Compare(x, y);

        #endregion CompareTo/Compare

        // ReSharper disable once InconsistentNaming
        static bool timestamp2tm(long dt, out pg_tm tm, out int fsec)
        {
            tm = new pg_tm();
            fsec = default;

            var date = dt / MicroSecsPerDay;
            var time = date != 0L ? dt - date * MicroSecsPerDay : dt;

            if (time < 0L)
            {
                time += MicroSecsPerDay;
                date -= 1L;
            }

            /* add offset to go from J2000 back to standard Julian date */
            date += PostgresEpochJulianDate;

            /* Julian day routine does not work for negative Julian days */
            if (date < 0 || date > int.MaxValue)
                return false;//throw new ArgumentOutOfRangeException();


            j2date((int) date, out tm.tm_year, out tm.tm_mon, out tm.tm_mday);
            dt2time(time, out tm.tm_hour, out tm.tm_min, out tm.tm_sec, out fsec);

            return true;
        }

        static void dt2time(long jd, out int hour, out int min, out int sec, out int fsec)
        {
            var time = jd;
            hour = (int)(time / MicroSecsPerHour);
            time -= hour * MicroSecsPerHour;
            min = (int)(time / MicroSecsPerMinute);
            time -= min * MicroSecsPerMinute;
            sec = (int)(time / MicroSecsPerSec);
            fsec = (int)(time - sec * MicroSecsPerSec);
        }

        // See src/backend/utils/adt/datetime.c
        // ReSharper disable once InconsistentNaming
        static void j2date(int jd, out int year, out int month, out int day)
        {
            unchecked
            {
                var julian = (uint)jd;
                julian += 32044;
                var quad = julian / 146097;
                var extra = (julian - quad * 146097) * 4 + 3;
                julian += 60 + quad * 3 + extra / 146097;
                quad = julian / 1461;
                julian -= quad * 1461;
                var y = (int)(julian * 4u / 1461u);
                julian = ((y != 0)
                    ? ((julian + 305) % 365)
                    : ((julian + 306) % 366)) + 123;
                y += (int)(quad * 4u);
                year = y - 4800;
                quad = julian * 2141 / 65536;
                day = (int)(julian - 7834U * quad / 256);
                month = (int)((quad + 10) % MonthsPerYear + 1);
            }
        }	

        // ReSharper disable once InconsistentNaming
        ref struct pg_tm
        {
            public int tm_sec;
            public int tm_min;
            public int tm_hour;
            public int tm_mday;
            public int tm_mon;			/* origin 1, not 0! */
            public int tm_year;		/* relative to 1900 */
            public int tm_wday;
            //public int tm_yday;
            //public int tm_isdst;
            //public long	tm_gmtoff;
            //public string tm_zone;
        }
    }
}
#endif // !LegacyProviderSpecificDateTimeTypes
