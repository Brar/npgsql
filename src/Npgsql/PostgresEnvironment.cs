using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Npgsql;

static class PostgresEnvironment
{
    // In the order they appear in https://www.postgresql.org/docs/current/libpq-envars.html
    public static string? Host => Environment.GetEnvironmentVariable("PGHOST");
    public static string? HostAddress => Environment.GetEnvironmentVariable("PGHOSTADDR");
    public static string? Port => Environment.GetEnvironmentVariable("PGPORT");
    public static string? Database => Environment.GetEnvironmentVariable("PGDATABASE");
    public static string? User => Environment.GetEnvironmentVariable("PGUSER");
    public static string? Password => Environment.GetEnvironmentVariable("PGPASSWORD");
    public static string? PassFile => Environment.GetEnvironmentVariable("PGPASSFILE");
    public static string? Options => Environment.GetEnvironmentVariable("PGOPTIONS");
    public static string? ApplicationName => Environment.GetEnvironmentVariable("PGAPPNAME");
    public static string? SslMode => Environment.GetEnvironmentVariable("PGSSLMODE");
    public static string? SslCert => Environment.GetEnvironmentVariable("PGSSLCERT");
    public static string? SslKey => Environment.GetEnvironmentVariable("PGSSLKEY");
    public static string? SslCertRoot => Environment.GetEnvironmentVariable("PGSSLROOTCERT");
    public static string? KerberosServiceName => Environment.GetEnvironmentVariable("PGKRBSRVNAME");
    public static string? ConnectTimeout => Environment.GetEnvironmentVariable("PGCONNECT_TIMEOUT");
    public static string? ClientEncoding => Environment.GetEnvironmentVariable("PGCLIENTENCODING");
    public static string? TargetSessionAttributes => Environment.GetEnvironmentVariable("PGTARGETSESSIONATTRS");
    public static string? TimeZone => Environment.GetEnvironmentVariable("PGTZ");
    

    internal static string? PassFileDefault
        => (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? GetHomePostgresDir() : GetHomeDir()) is string homedir &&
           Path.Combine(homedir, RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "pgpass.conf" : ".pgpass") is var path &&
           File.Exists(path)
            ? path
            : null;


    internal static string? SslCertDefault
        => GetHomePostgresDir() is string homedir && Path.Combine(homedir, "postgresql.crt") is var path && File.Exists(path)
            ? path
            : null;


    internal static string? SslKeyDefault
        => GetHomePostgresDir() is string homedir && Path.Combine(homedir, "postgresql.key") is var path && File.Exists(path)
            ? path
            : null;


    internal static string? SslCertRootDefault
        => GetHomePostgresDir() is string homedir && Path.Combine(homedir, "root.crt") is var path && File.Exists(path)
            ? path
            : null;


    static string? GetHomeDir()
        => Environment.GetEnvironmentVariable(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "APPDATA" : "HOME");

    static string? GetHomePostgresDir()
        => GetHomeDir() is string homedir
            ? Path.Combine(homedir, RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "postgresql" : ".postgresql")
            : null;
}