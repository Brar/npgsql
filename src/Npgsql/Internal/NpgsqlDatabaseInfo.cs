using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Npgsql.PostgresTypes;
using Npgsql.Util;

namespace Npgsql.Internal
{
    /// <summary>
    /// Base class for implementations which provide information about PostgreSQL and PostgreSQL-like databases
    /// (e.g. type definitions, capabilities...).
    /// </summary>
    public abstract class NpgsqlDatabaseInfo
    {
        #region Fields

        internal static readonly ConcurrentDictionary<NpgsqlDatabaseInfoCacheKey, NpgsqlDatabaseInfo> Cache
            = new();

        static volatile INpgsqlDatabaseInfoFactory[] Factories = new INpgsqlDatabaseInfoFactory[]
        {
            new PostgresMinimalDatabaseInfoFactory(),
            new PostgresDatabaseInfoFactory()
        };

        readonly string? _serverVersion;
        readonly string? _portableServerVersion;

        #endregion Fields

        #region General database info

        /// <summary>
        /// The hostname of IP address of the database.
        /// </summary>
        public string Host { get; }

        /// <summary>
        /// The TCP port of the database.
        /// </summary>
        public int Port { get; }

        /// <summary>
        /// The database name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// The version of the PostgreSQL database we're connected to.
        /// In case of a development or pre-release version this field will contain
        /// the version of the next version to be released from this branch.
        /// Exposed via <see cref="NpgsqlConnection.PostgreSqlVersion"/>.
        /// </summary>
        public Version Version { get; }

        /// <summary>
        /// The server version of the PostgreSQL database we're connected to.
        /// Exposed via <see cref="NpgsqlConnection.ServerVersion"/>.
        /// </summary>
        public string ServerVersion => _serverVersion ?? Version.ToString();

        /// <summary>
        /// The server version of the PostgreSQL database we're connected to.
        /// Exposed via <see cref="NpgsqlConnection.ServerVersion"/>.
        /// </summary>
        internal string PortableServerVersion => _portableServerVersion ?? Version.ToString();

        /// <summary>
        /// The pre-release version of the PostgreSQL database we're connected to
        /// (e. g. '3' for 9.4beta3 or '1' for 9.4rc1) if it is an alpha, beta or
        /// release candidate version; otherwise <see langword="null"/>.
        /// </summary>
        internal int? PreReleaseVersion { get; }

        /// <summary>
        /// <see langword="true"/> if the PostgreSQL database we're
        /// connected to is a release; otherwise <see langword="false"/>.
        /// </summary>
        internal bool IsRelease => ReleaseType == ReleaseType.Release;

        /// <summary>
        /// <see langword="true"/> if the PostgreSQL database we're
        /// connected to is a development version; otherwise <see langword="false"/>.
        /// </summary>
        internal ReleaseType ReleaseType { get; }

        #endregion General database info

        #region Supported capabilities and features

        /// <summary>
        /// Whether the backend supports range types.
        /// </summary>
        public virtual bool SupportsRangeTypes => Version.IsGreaterOrEqual(9, 2);
        /// <summary>
        /// Whether the backend supports enum types.
        /// </summary>
        public virtual bool SupportsEnumTypes => Version.IsGreaterOrEqual(8, 3);
        /// <summary>
        /// Whether the backend supports the CLOSE ALL statement.
        /// </summary>
        public virtual bool SupportsCloseAll => Version.IsGreaterOrEqual(8, 3);
        /// <summary>
        /// Whether the backend supports advisory locks.
        /// </summary>
        public virtual bool SupportsAdvisoryLocks => Version.IsGreaterOrEqual(8, 2);
        /// <summary>
        /// Whether the backend supports the DISCARD SEQUENCES statement.
        /// </summary>
        public virtual bool SupportsDiscardSequences => Version.IsGreaterOrEqual(9, 4);
        /// <summary>
        /// Whether the backend supports the UNLISTEN statement.
        /// </summary>
        public virtual bool SupportsUnlisten => Version.IsGreaterOrEqual(6, 4);  // overridden by PostgresDatabase
        /// <summary>
        /// Whether the backend supports the DISCARD TEMP statement.
        /// </summary>
        public virtual bool SupportsDiscardTemp => Version.IsGreaterOrEqual(8, 3);
        /// <summary>
        /// Whether the backend supports the DISCARD statement.
        /// </summary>
        public virtual bool SupportsDiscard => Version.IsGreaterOrEqual(8, 3);

        /// <summary>
        /// Reports whether the backend uses the newer integer timestamp representation.
        /// </summary>
        public virtual bool HasIntegerDateTimes { get; protected set; } = true;

        /// <summary>
        /// Whether the database supports transactions.
        /// </summary>
        public virtual bool SupportsTransactions { get; protected set; } = true;

        #endregion Supported capabilities and features

        #region Types

        readonly List<PostgresBaseType>      _baseTypesMutable      = new();
        readonly List<PostgresArrayType>     _arrayTypesMutable     = new();
        readonly List<PostgresRangeType>     _rangeTypesMutable     = new();
        readonly List<PostgresEnumType>      _enumTypesMutable      = new();
        readonly List<PostgresCompositeType> _compositeTypesMutable = new();
        readonly List<PostgresDomainType>    _domainTypesMutable    = new();

        internal IReadOnlyList<PostgresBaseType>      BaseTypes      => _baseTypesMutable;
        internal IReadOnlyList<PostgresArrayType>     ArrayTypes     => _arrayTypesMutable;
        internal IReadOnlyList<PostgresRangeType>     RangeTypes     => _rangeTypesMutable;
        internal IReadOnlyList<PostgresEnumType>      EnumTypes      => _enumTypesMutable;
        internal IReadOnlyList<PostgresCompositeType> CompositeTypes => _compositeTypesMutable;
        internal IReadOnlyList<PostgresDomainType>    DomainTypes    => _domainTypesMutable;

        /// <summary>
        /// Indexes backend types by their type OID.
        /// </summary>
        internal Dictionary<uint, PostgresType> ByOID { get; } = new();

        /// <summary>
        /// Indexes backend types by their PostgreSQL name, including namespace (e.g. pg_catalog.int4).
        /// Only used for enums and composites.
        /// </summary>
        internal Dictionary<string, PostgresType> ByFullName { get; } = new();

        /// <summary>
        /// Indexes backend types by their PostgreSQL name, not including namespace.
        /// If more than one type exists with the same name (i.e. in different namespaces) this
        /// table will contain an entry with a null value.
        /// Only used for enums and composites.
        /// </summary>
        internal Dictionary<string, PostgresType?> ByName { get; } = new();

        /// <summary>
        /// Initializes the instance of <see cref="NpgsqlDatabaseInfo"/>.
        /// </summary>
        protected NpgsqlDatabaseInfo(string host, int port, string databaseName, Version version)
        {
            Host = host;
            Port = port;
            Name = databaseName;
            Version = version;
            _serverVersion = null;
            _portableServerVersion = null;
        }

        /// <summary>
        /// Initializes the instance of <see cref="NpgsqlDatabaseInfo"/>.
        /// </summary>
        private protected NpgsqlDatabaseInfo(string host, int port, string databaseName, string versionString)
        {
            Host = host;
            Port = port;
            Name = databaseName;
            _serverVersion = versionString;
            (Version, _portableServerVersion, ReleaseType, PreReleaseVersion) = ParseVersionString(versionString);
        }

        internal void ProcessTypes()
        {
            foreach (var type in GetTypes())
            {
                ByOID[type.OID] = type;
                ByFullName[type.FullName] = type;
                // If more than one type exists with the same partial name, we place a null value.
                // This allows us to detect this case later and force the user to use full names only.
                ByName[type.Name] = ByName.ContainsKey(type.Name)
                    ? null
                    : type;

                switch (type)
                {
                case PostgresBaseType baseType:
                    _baseTypesMutable.Add(baseType);
                    continue;
                case PostgresArrayType arrayType:
                    _arrayTypesMutable.Add(arrayType);
                    continue;
                case PostgresRangeType rangeType:
                    _rangeTypesMutable.Add(rangeType);
                    continue;
                case PostgresEnumType enumType:
                    _enumTypesMutable.Add(enumType);
                    continue;
                case PostgresCompositeType compositeType:
                    _compositeTypesMutable.Add(compositeType);
                    continue;
                case PostgresDomainType domainType:
                    _domainTypesMutable.Add(domainType);
                    continue;
                default:
                    throw new ArgumentOutOfRangeException();
                }
            }
        }

        /// <summary>
        /// Provides all PostgreSQL types detected in this database.
        /// </summary>
        /// <returns></returns>
        protected abstract IEnumerable<PostgresType> GetTypes();

        #endregion Types

        #region Misc

        /// <summary>
        /// Parses a PostgreSQL server version (e.g. 10.1, 9.6.3) and returns a CLR Version.
        /// </summary>
        protected static Version ParseServerVersion(string value)
        {
            var versionSpan = value.AsSpan().TrimStart();
            for (var idx = 0; idx != versionSpan.Length; ++idx)
            {
                var c = versionSpan[idx];
                if (!char.IsDigit(c) && c != '.')
                {
                    versionSpan = versionSpan.Slice(0, idx);
                    break;
                }
            }
            if (!versionSpan.Contains(".".AsSpan(), StringComparison.Ordinal))
                return new Version(versionSpan.ToString() + ".0");

            return new Version(versionSpan.ToString());
        }

        /// <summary>
        /// Takes the PostgreSQL server_version return value and extracts the
        /// version string including the beta and development status
        /// </summary>
        internal static (Version Version, string PortableServerVersion, ReleaseType ReleaseType, int? PreReleaseVersion)
            ParseVersionString(string versionString)
        {
            var str = versionString.AsSpan();
            var state = VersionParseState.Initial;
            var serverVersionSliceStart = -1;
            var digitsSliceStart = -1;
            var digitsCount = 0;
            var major = -1;
            var minor = -1;
            var build = -1;
            var revision = -1;
            var releaseType = (ReleaseType)(-1);
            for (var idx = 0; idx < str.Length; idx++)
            {
                var c = str[idx];
                switch (state)
                {
                case VersionParseState.Initial:
                {
                    if (char.IsWhiteSpace(c))
                        continue;
                    if (!char.IsDigit(c))
                        throw FormatException(str, idx);

                    state = VersionParseState.MajorDigitsRead;
                    serverVersionSliceStart = idx;
                    digitsSliceStart = idx;
                    digitsCount = 1;
                    continue;
                }
                case VersionParseState.MajorDigitsRead:
                {
                    switch (c)
                    {
                    case '.':
                        state = VersionParseState.MinorDigitsStart;
                        break;
                    case 'a':
                        state = VersionParseState.AlphaStringRead;
                        break;
                    case 'b':
                        state = VersionParseState.BetaStringRead;
                        break;
                    case 'd':
                        state = VersionParseState.DevelStringRead;
                        break;
                    case 'r':
                        state = VersionParseState.RcStringRead;
                        break;
                    default:
                        if (char.IsDigit(c))
                        {
                            digitsCount++;
                            continue;
                        }
                        // Done.
                        // We have major and the next character doesn't indicate a build number,
                        // a beta or a development version 
                        return (Version: CreateVersion(ParseInt(str.Slice(digitsSliceStart, digitsCount))),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, idx).ToString(),
                            ReleaseType: ReleaseType.Release,
                            PreReleaseVersion: null);
                    }

                    major = ParseInt(str.Slice(digitsSliceStart, digitsCount));
                    continue;
                }
                case VersionParseState.MinorDigitsStart:
                {
                    if (!char.IsDigit(c))
                        throw FormatException(str, idx);
                    state = VersionParseState.MinorDigitsRead;
                    digitsSliceStart = idx;
                    digitsCount = 1;
                    continue;
                }
                case VersionParseState.MinorDigitsRead:
                {
                    switch (c)
                    {
                    case '.':
                        state = VersionParseState.BuildDigitsStart;
                        break;
                    case 'a':
                        state = VersionParseState.AlphaStringRead;
                        break;
                    case 'b':
                        state = VersionParseState.BetaStringRead;
                        break;
                    case 'd':
                        state = VersionParseState.DevelStringRead;
                        break;
                    case 'r':
                        state = VersionParseState.RcStringRead;
                        break;
                    default:
                        if (char.IsDigit(c))
                        {
                            digitsCount++;
                            continue;
                        }

                        // Done.
                        // We have major and minor and the next character doesn't indicate a build number,
                        // a pre-release or a development version 
                        return (Version: CreateVersion(major, ParseInt(str.Slice(digitsSliceStart, digitsCount))),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, idx).ToString(),
                            ReleaseType: ReleaseType.Release,
                            PreReleaseVersion: null);
                    }

                    minor = ParseInt(str.Slice(digitsSliceStart, digitsCount));
                    continue;
                }
                case VersionParseState.BuildDigitsStart:
                {
                    if (!char.IsDigit(c))
                        throw FormatException(str, idx);
                    state = VersionParseState.BuildDigitsRead;
                    digitsSliceStart = idx;
                    digitsCount = 1;
                    continue;
                }
                case VersionParseState.BuildDigitsRead:
                {
                    switch (c)
                    {
                    case '.':
                        state = VersionParseState.RevisionDigitsStart;
                        break;
                    case 'a':
                        state = VersionParseState.AlphaStringRead;
                        break;
                    case 'b':
                        state = VersionParseState.BetaStringRead;
                        break;
                    case 'd':
                        state = VersionParseState.DevelStringRead;
                        break;
                    case 'r':
                        state = VersionParseState.RcStringRead;
                        break;
                    default:
                        if (char.IsDigit(c))
                        {
                            digitsCount++;
                            continue;
                        }

                        // Done.
                        // We have major, minor and build and the next character doesn't indicate
                        // a pre-release or a development version
                        // We don't expect a revision number since PostgreSQL doesn't use them
                        return (Version: CreateVersion(major, minor, ParseInt(str.Slice(digitsSliceStart, digitsCount))),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, idx).ToString(),
                            ReleaseType: ReleaseType.Release,
                            PreReleaseVersion: null);
                    }

                    build = ParseInt(str.Slice(digitsSliceStart, digitsCount));
                    continue;
                }
                case VersionParseState.RevisionDigitsStart:
                {
                    if (!char.IsDigit(c))
                        throw FormatException(str, idx);
                    state = VersionParseState.RevisionDigitsRead;
                    digitsSliceStart = idx;
                    digitsCount = 1;
                    continue;
                }
                case VersionParseState.RevisionDigitsRead:
                {
                    switch (c)
                    {
                    case '.':
                        throw FormatException(str, idx);
                    case 'a':
                        state = VersionParseState.AlphaStringRead;
                        break;
                    case 'b':
                        state = VersionParseState.BetaStringRead;
                        break;
                    case 'd':
                        state = VersionParseState.DevelStringRead;
                        break;
                    case 'r':
                        state = VersionParseState.RcStringRead;
                        break;
                    default:
                        if (char.IsDigit(c))
                        {
                            digitsCount++;
                            continue;
                        }

                        // Done.
                        // We have major, minor, build and revision and the next character doesn't indicate
                        // a pre-release or a development version
                        // We don't expect a revision number since PostgreSQL doesn't use them
                        return (Version: CreateVersion(major, minor, build, ParseInt(str.Slice(digitsSliceStart, digitsCount))),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, idx).ToString(),
                            ReleaseType: ReleaseType.Release,
                            PreReleaseVersion: null);
                    }

                    revision = ParseInt(str.Slice(digitsSliceStart, digitsCount));
                    continue;
                }
                case VersionParseState.DevelStringRead:
                {
                    var serverVersionSliceSavePoint = idx - 1;
                    // If our attempt to read the string 'devel' failed we return
                    // the version we've parsed already
                    if (c != 'e'
                        || ++idx >= str.Length || str[idx] != 'v'
                        || ++idx >= str.Length || str[idx] != 'e'
                        || ++idx >= str.Length || str[idx] != 'l')
                        return (Version: CreateVersion(major, minor),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, serverVersionSliceSavePoint).ToString(),
                            ReleaseType: ReleaseType.Release,
                            PreReleaseVersion: null);

                    return (Version: CreateVersion(major, minor, build, revision),
                        PortableServerVersion: str.Slice(serverVersionSliceStart, idx + 1).ToString(),
                        ReleaseType: ReleaseType.Devel,
                        PreReleaseVersion: null);
                }
                case VersionParseState.AlphaStringRead:
                {
                    var serverVersionSliceSavePoint = idx - 1;
                    if (c != 'l'
                        || ++idx == str.Length || str[idx] != 'p'
                        || ++idx == str.Length || str[idx] != 'h'
                        || ++idx == str.Length || str[idx] != 'a')
                    {
                        // If our attempt to read the string 'alpha' failed we return
                        // the version we've parsed already
                        return (Version: CreateVersion(major, minor, build, revision),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, serverVersionSliceSavePoint).ToString(),
                            ReleaseType: ReleaseType.Release,
                            PreReleaseVersion: null);
                    }

                    // The alpha version doesn't have the expected digit.
                    // Succeed anyways and set PreReleaseVersion to null
                    if (++idx == str.Length || !char.IsDigit(str[idx]))
                        return (Version: CreateVersion(major, minor, build, revision),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, idx).ToString(),
                            ReleaseType: ReleaseType.Alpha,
                            PreReleaseVersion: null);

                    state = VersionParseState.PreReleaseVersionDigitsRead;
                    releaseType = ReleaseType.Alpha;
                    digitsSliceStart = idx;
                    digitsCount = 1;
                    continue;
                }
                case VersionParseState.BetaStringRead:
                {
                    var serverVersionSliceSavePoint = idx - 1;
                    if (c != 'e'
                        || ++idx == str.Length || str[idx] != 't'
                        || ++idx == str.Length || str[idx] != 'a')
                    {
                        // If our attempt to read the string 'beta' failed we return
                        // the version we've parsed already
                        return (Version: CreateVersion(major, minor, build, revision),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, serverVersionSliceSavePoint).ToString(),
                            ReleaseType: ReleaseType.Release,
                            PreReleaseVersion: null);
                    }

                    // The beta version doesn't have the expected digit.
                    // Succeed anyways and set PreReleaseVersion to null
                    if (++idx == str.Length || !char.IsDigit(str[idx]))
                        return (Version: CreateVersion(major, minor, build, revision),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, idx).ToString(),
                            ReleaseType: ReleaseType.Beta,
                            PreReleaseVersion: null);

                    state = VersionParseState.PreReleaseVersionDigitsRead;
                    releaseType = ReleaseType.Beta;
                    digitsSliceStart = idx;
                    digitsCount = 1;
                    continue;
                }
                case VersionParseState.RcStringRead:
                {
                    if (c != 'c')
                    {
                        // If our attempt to read the string 'rc' failed we return
                        // the version we've parsed already
                        return (Version: CreateVersion(major, minor, build, revision),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, idx - 1).ToString(),
                            ReleaseType: ReleaseType.Release,
                            PreReleaseVersion: null);
                    }

                    // The release candidate doesn't have the expected digit.
                    // Succeed anyways and set PreReleaseVersion to null
                    if (++idx == str.Length || !char.IsDigit(str[idx]))
                        return (Version: CreateVersion(major, minor, build, revision),
                            PortableServerVersion: str.Slice(serverVersionSliceStart, idx).ToString(),
                            ReleaseType: ReleaseType.ReleaseCandidate,
                            PreReleaseVersion: null);

                    state = VersionParseState.PreReleaseVersionDigitsRead;
                    releaseType = ReleaseType.ReleaseCandidate;
                    digitsSliceStart = idx;
                    digitsCount = 1;
                    continue;
                }
                case VersionParseState.PreReleaseVersionDigitsRead:
                {
                    if (char.IsDigit(c))
                    {
                        digitsCount++;
                        continue;
                    }

                    return (Version: CreateVersion(major, minor, build, revision),
                        PortableServerVersion: str.Slice(serverVersionSliceStart, idx).ToString(),
                        ReleaseType: releaseType,
                        PreReleaseVersion: ParseInt(str.Slice(digitsSliceStart, digitsCount)));
                }
                default:
                    throw FormatException(str, idx);
                }
            }

            // The version string may end while we are reading digits
            // in which case we parse the digits we already have
            switch (state)
            {
            case VersionParseState.MajorDigitsRead:
                return (Version: CreateVersion(ParseInt(str.Slice(digitsSliceStart))),
                    PortableServerVersion: str.Slice(serverVersionSliceStart).ToString(),
                    ReleaseType: ReleaseType.Release,
                    PreReleaseVersion: null);
            case VersionParseState.MinorDigitsRead:
                return (Version: CreateVersion(major, ParseInt(str.Slice(digitsSliceStart))),
                    PortableServerVersion: str.Slice(serverVersionSliceStart).ToString(),
                    ReleaseType: ReleaseType.Release,
                    PreReleaseVersion: null);
            case VersionParseState.BuildDigitsRead:
                return (Version: CreateVersion(major, minor, ParseInt(str.Slice(digitsSliceStart))),
                    PortableServerVersion: str.Slice(serverVersionSliceStart).ToString(),
                    ReleaseType: ReleaseType.Release,
                    PreReleaseVersion: null);
            case VersionParseState.RevisionDigitsRead:
                return (Version: CreateVersion(major, minor, build, ParseInt(str.Slice(digitsSliceStart))),
                    PortableServerVersion: str.Slice(serverVersionSliceStart).ToString(),
                    ReleaseType: ReleaseType.Release,
                    PreReleaseVersion: null);
            case VersionParseState.PreReleaseVersionDigitsRead:
                return (Version: CreateVersion(major, minor, build, revision),
                    PortableServerVersion: str.Slice(serverVersionSliceStart).ToString(),
                    ReleaseType: releaseType,
                    PreReleaseVersion: ParseInt(str.Slice(digitsSliceStart)));
            default:
                throw new FormatException($"Failed to parse the server version string '{str.ToString()}'.");
            }

            static FormatException FormatException(ReadOnlySpan<char> str, int index)
                => new($"Failed to parse the server version string '{str.ToString()}'. Unexpected character '{str[index]}' at index {index}.");

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static Version CreateVersion(int major, int minor = 0, int build = -1, int revision = -1)
                => revision > -1
                    ? new(major, minor, build, revision)
                    : build > -1
                        ? new(major, minor, build)
                        : minor > -1
                            ? new(major, minor)
                            : new(major, 0);

            // Put the .NET Standard 2.0 ugliness in one place
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int ParseInt(ReadOnlySpan<char> str)
                => int.Parse(str
#if NETSTANDARD2_0
                        .ToString()
#endif
                );
        }

        enum VersionParseState
        {
            Initial,
            MajorDigitsRead,
            MinorDigitsStart,
            MinorDigitsRead,
            BuildDigitsStart,
            BuildDigitsRead,
            RevisionDigitsStart,
            RevisionDigitsRead,
            DevelStringRead,
            AlphaStringRead,
            BetaStringRead,
            RcStringRead,
            PreReleaseVersionDigitsRead,
        }

        #endregion Misc

        #region Factory management

        /// <summary>
        /// Registers a new database info factory, which is used to load information about databases.
        /// </summary>
        public static void RegisterFactory(INpgsqlDatabaseInfoFactory factory)
        {
            if (factory == null)
                throw new ArgumentNullException(nameof(factory));

            var factories = new INpgsqlDatabaseInfoFactory[Factories.Length + 1];
            factories[0] = factory;
            Array.Copy(Factories, 0, factories, 1, Factories.Length);
            Factories = factories;

            Cache.Clear();
        }

        internal static async Task<NpgsqlDatabaseInfo> Load(NpgsqlConnector conn, NpgsqlTimeout timeout, bool async)
        {
            foreach (var factory in Factories)
            {
                var dbInfo = await factory.Load(conn, timeout, async);
                if (dbInfo != null)
                {
                    dbInfo.ProcessTypes();
                    return dbInfo;
                }
            }

            // Should never be here
            throw new NpgsqlException("No DatabaseInfoFactory could be found for this connection");
        }

        // For tests
        internal static void ResetFactories()
        {
            Factories = new INpgsqlDatabaseInfoFactory[]
            {
                new PostgresMinimalDatabaseInfoFactory(),
                new PostgresDatabaseInfoFactory()
            };
            Cache.Clear();
        }

        #endregion Factory management
    }

    enum ReleaseType
    {
        Release,
        Alpha,
        Beta,
        ReleaseCandidate,
        Devel
    }
}
