using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Npgsql;

static class NpgsqlConnectionDefaults
{
    #region constants
    // Defaults that aren't listed here have their .NET default values as defaults (e.g. 0 for int)

    public const int AutoPrepareMinUsages = 5;
    public const int CancellationTimeout = 2000;
    public const string ClientEncoding = "UTF8";
    public const int CommandTimeout = 30;
    public const int ConnectionIdleLifetime = 300;
    public const int ConnectionPruningInterval = 10;
    public const string Encoding = "UTF8";
    public static readonly string Host =
#if NET5_0_OR_GREATER
        !Socket.OSSupportsUnixDomainSockets ||
#endif
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "localhost" : "/tmp";
    public const int InternalCommandTimeout = -1;
    public const string KerberosServiceName = "postgres";
    public const int MaxPoolSize = 100;
    public const bool Pooling = true;
    public const int Port = 5432;
    public const int ReadBufferSize = 8192;
    public const SslMode SslMode = Npgsql.SslMode.Prefer;
    public const int Timeout = 15;
    public const int WriteBufferSize = 8192;
    public const int WriteCoalescingBufferThresholdBytes = 1000;

    #endregion constants
    
    public static string? InferApplicationName(string? applicationName)
        => applicationName ?? PostgresEnvironment.ApplicationName;
    
    public static string InferClientEncoding(string? clientEncoding)
        => clientEncoding ?? PostgresEnvironment.ClientEncoding ?? ClientEncoding;

    public static string InferDatabase(string? database, string inferredUserName)
        => database ?? PostgresEnvironment.Database ?? inferredUserName;

    public static string InferDatabase(string? database, string? userName, bool includeRealm, ILogger connectionLogger)
        => database ?? PostgresEnvironment.Database ?? InferUser(userName, includeRealm, connectionLogger);

#if NET5_0_OR_GREATER
    public static ValueTask<string> InferDatabaseAsync(string? database, string? userName, bool includeRealm, ILogger connectionLogger)
    {
        if (database != null)
            return ValueTask.FromResult(database);

        var environmentDatabase = PostgresEnvironment.Database;
        return environmentDatabase == null
            ? InferUserAsync(userName, includeRealm, connectionLogger)
            : ValueTask.FromResult(environmentDatabase);
    }
#endif

    public static string InferEncoding(string? encoding)
        => encoding ?? ClientEncoding;

    public static (string?[]? Hosts, IPAddress?[]? HostAddresses, int[] Ports) InferHostsAndHostAddressesAndPorts(string? host, string? hostAddress, int port)
    {
        host ??= PostgresEnvironment.Host;
        hostAddress ??= PostgresEnvironment.HostAddress;

        if (host == null && hostAddress == null)
            return (new[] { Host }, null, InferPorts(port));
        if (hostAddress == null)
            return ReadComplete(host!, null, InferPorts(port));
        return host == null
            ? (null, ReadHostAddresses(hostAddress), InferPorts(port))
            : ReadComplete(host, hostAddress, InferPorts(port));

        // This method supports our "host=host:port,host:port" syntax as well as PostgreSQL's "host=host,host" "port=port,port" syntax
        static (string?[] Hosts, IPAddress?[]? HostAddresses, int[] Ports) ReadComplete(string host, string? hostAddress, int[] ports)
        {
            var hostParts = host.Split(',');
            var isSinglePort = ports.Length == 1;
            if (!isSinglePort && ports.Length != hostParts.Length)
                throw new ArgumentException(
                    "If multiple ports are specified their number has to match the number of hosts.", nameof(ports));

            var hostAddresses = hostAddress == null ? null : ReadHostAddresses(hostAddress);
            if (hostAddresses != null && hostAddresses.Length != hostParts.Length)
                throw new ArgumentException(
                    $"If both, host and hostaddr are specified they have to contain the same number of entries.");

            if (hostParts.Length == 1)
                return (new[] { host }, hostAddresses, ports);

            var singlePort = ports[0];
            var newPorts = new int[hostParts.Length];
            var newHosts = new string?[hostParts.Length];
            for (var i = 0; i < hostParts.Length; i++)
            {
                var currentHost = hostParts[i];
                if (NpgsqlConnectionStringBuilder.TrySplitHostPort(currentHost.AsSpan(), out var newHost, out var newPort))
                {
                    if (!isSinglePort)
                        throw new InvalidOperationException(
                            "Either use multiple ports in the PGPORT environment variable or specify them as part of the host. Doing both is not supported.");
                    newHosts[i] = newHost.Length < 1 ? Host : newHost;
                    newPorts[i] = newPort;
                }
                else
                {
                    newHosts[i] = currentHost.Length < 1 ? Host : currentHost;
                    newPorts[i] = isSinglePort ? singlePort : ports[i];
                }
            }

            return (newHosts, hostAddresses, newPorts);
        }

        // Currently hostaddr may only come from the PGHOSTADDR environment variable and not from the connection string
        // Because of this we don't have to support a "hostaddr:port" syntax as that's not supported by PostgreSQL.
        // We might decide to add hostaddr support to the connection string at some point in the future, at which point we
        // should probably also support the "hostaddr:port" syntax
        static IPAddress?[] ReadHostAddresses(string hostAddress)
        {
            var hostAddressParts = hostAddress.Split(',');
            if (hostAddressParts.Length == 1)
                return new[] { IPAddress.Parse(hostAddress) };

            var hostAddresses = new IPAddress?[hostAddressParts.Length];
            for (var i = hostAddresses.Length - 1; i >= 0; i--)
            {
                var addr = hostAddressParts[i];
                hostAddresses[i] = addr.Length < 1 ? null : IPAddress.Parse(addr);
            }

            return hostAddresses;
        }

        static int[] InferPorts(int port)
        {
            if (port != 0)
                return new[] { port };

            var envPort = PostgresEnvironment.Port;
            if (envPort == null)
                return new[] { Port };

            if (!envPort.Contains(","))
                return new int[] { ushort.Parse(envPort) };

            var parts = envPort.Split(',');
            var ports = new int[parts.Length];
            for (var i = parts.Length - 1; i >= 0; i--)
            {
                var portPart = parts[i];
                ports[i] = portPart?.Length < 1 ? Port : ushort.Parse(portPart!);
            }
            return ports;
        }
    }

    public static string InferKerberosServiceName(string? kerberosServiceName)
        => kerberosServiceName ?? PostgresEnvironment.KerberosServiceName ?? KerberosServiceName;

    public static string? InferOptions(string? options)
        => options ?? PostgresEnvironment.Options;

    public static string? InferPassFile(string? passFile)
        => passFile ?? PostgresEnvironment.PassFile;

    public static string? InferPassword(string? password)
        => password ?? PostgresEnvironment.Password;

    public static string? InferTimeZone(string? timeZone)
        => timeZone ?? PostgresEnvironment.TimeZone;

    public static string InferUser(string? user, bool includeRealm, ILogger connectionLogger)
        => user ?? PostgresEnvironment.User ??
            (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? KerberosUsernameProvider.GetUsername(includeRealm, connectionLogger) ?? Environment.UserName
                : Environment.UserName);

#if NET5_0_OR_GREATER
    public static async ValueTask<string> InferUserAsync(string? user, bool includeRealm, ILogger connectionLogger)
    {
        if (user != null)
            return user;

        var environmentUser = PostgresEnvironment.User;
        if (environmentUser != null)
            return environmentUser;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return Environment.UserName;

        return await KerberosUsernameProvider.GetUsernameAsync(includeRealm, connectionLogger) ?? Environment.UserName;
    }
#endif
}
