using System;

namespace Npgsql;

readonly struct NpgsqlDatabaseInfoCacheKey : IEquatable<NpgsqlDatabaseInfoCacheKey>
{
    public readonly int Port;
    public readonly string? Host;
    public readonly string? Database;
    public readonly ServerCompatibilityMode CompatibilityMode;

    public NpgsqlDatabaseInfoCacheKey(NpgsqlSingleHostConnectionSettings settings)
    {
        Port = settings.Port;
        Host = settings.Host;
        Database = settings.Database;
        CompatibilityMode = settings.ServerCompatibilityMode;
    }

    public bool Equals(NpgsqlDatabaseInfoCacheKey other) =>
        Port == other.Port &&
        Host == other.Host &&
        Database == other.Database &&
        CompatibilityMode == other.CompatibilityMode;

    public override bool Equals(object? obj) =>
        obj is NpgsqlDatabaseInfoCacheKey key && key.Equals(this);

    public override int GetHashCode() =>
        HashCode.Combine(Port, Host, Database, CompatibilityMode);
}