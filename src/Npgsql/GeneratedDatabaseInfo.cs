using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Internal;
using Npgsql.PostgresTypes;
using Npgsql.Util;

namespace Npgsql;

class GeneratedDatabaseInfoFactory : INpgsqlDatabaseInfoFactory
{
    public Task<NpgsqlDatabaseInfo?> Load(NpgsqlConnector conn, NpgsqlTimeout timeout, bool async)
        => Task.FromResult(
            conn.Settings.ServerCompatibilityMode == ServerCompatibilityMode.AllHardCodedTypes
                ? (NpgsqlDatabaseInfo)new GeneratedDatabaseInfo(conn)
                : null
        );
}


partial class GeneratedDatabaseInfo : PostgresDatabaseInfo
{
    protected override IEnumerable<PostgresType> GetTypes() => Types.Value;

    internal GeneratedDatabaseInfo(NpgsqlConnector conn)
        : base(conn)
    {
        HasIntegerDateTimes = !conn.PostgresParameters.TryGetValue("integer_datetimes", out var intDateTimes) ||
                              intDateTimes == "on";
    }
}
