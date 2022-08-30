using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

// ReSharper disable UnusedAutoPropertyAccessor.Global

namespace Npgsql;

#pragma warning disable CS1591

public sealed record NpgsqlSingleHostConnectionSettings : NpgsqlConnectionSettings
{
    internal NpgsqlSingleHostConnectionSettings(string clientEncoding, string database, string dataSourceCached, string encoding, string kerberosServiceName, string host, int port)
        : this(clientEncoding, database, dataSourceCached, encoding, kerberosServiceName, port, host: host) { }
    
    internal NpgsqlSingleHostConnectionSettings(string clientEncoding, string database, string dataSourceCached, string encoding, string kerberosServiceName, IPAddress hostaddr, int port)
        : this(clientEncoding, database, dataSourceCached, encoding, kerberosServiceName, port, hostaddr: hostaddr) { }
    
    internal NpgsqlSingleHostConnectionSettings(string clientEncoding, string database, string dataSourceCached, string encoding, string kerberosServiceName, string host, IPAddress hostaddr, int port)
        : this(clientEncoding, database, dataSourceCached, encoding, kerberosServiceName, port, host: host, hostaddr: hostaddr) { }

    NpgsqlSingleHostConnectionSettings(string clientEncoding, string database, string dataSourceCached, string encoding, string kerberosServiceName, int port, string? host = null, IPAddress? hostaddr = null)
        : base(clientEncoding, database, dataSourceCached, encoding, kerberosServiceName)
    {
        if (host == null && hostaddr == null)
            throw new ArgumentNullException($"Either {nameof(host)} or {nameof(hostaddr)} must be set.");

        if (host?.Contains(",") is true)
            throw new ArgumentException($"{nameof(host)} must be a single host name");

        Host = host;
        Port = port;
        HostAddress = hostaddr;
        if (hostaddr != null)
            EndPoint = new IPEndPoint(hostaddr, port);

        if (host != null && IPAddress.TryParse(host, out var hostIp))
            EndPoint = new IPEndPoint(hostIp, port);
        else if (host != null && NpgsqlConnectionStringBuilder.IsUnixSocket(host, port, out var socketPath))
            EndPoint = new UnixDomainSocketEndPoint(socketPath);
    }

    internal NpgsqlSingleHostConnectionSettings(NpgsqlMultiHostConnectionSettings settings)
        : base(settings)
    {
        Debug.Assert(settings.Hosts.Length == 1 || settings.HostAddresses.Length == 1);
        Debug.Assert(settings.EndPoints[0] != null || settings.Hosts[0] != null);
        Host = settings.Hosts.Length > 0 ? settings.Hosts[0] : null;
        HostAddress = settings.HostAddresses.Length > 0 ? settings.HostAddresses[0] : null;
        Debug.Assert(settings.Ports.Length == 1);
        Port = settings.Ports[0];
        Debug.Assert(settings.EndPoints.Length == 1);
        EndPoint = settings.EndPoints[0];
    }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Host"/>
    public string? Host { get; }

    /// <summary>
    /// The ip address to connect to
    /// </summary>
    public IPAddress? HostAddress { get; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Port"/>
    public int Port { get; }

    /// <summary>
    /// The endpoint to connect to
    /// </summary>
    internal EndPoint? EndPoint { get; }

    /// <inheritdoc cref="NpgsqlConnectionSettings.MultipleHosts"/>
    internal override bool MultipleHosts => false;

    /// <inheritdoc cref="NpgsqlConnectionSettings.SafeLogHost"/>
    internal override string SafeLogHost => Host ?? HostAddress!.ToString();

    /// <inheritdoc cref="NpgsqlConnectionSettings.SafeLogPort"/>
    internal override string SafeLogPort => Port.ToString(CultureInfo.InvariantCulture);

    public override string CreateConnectionString(PasswordHandling passwordHandling = PasswordHandling.Auto,
        DefaultHandling defaultHandling = DefaultHandling.OmitHardcodedDefaults) =>
        throw new NotImplementedException();

    public override string ToString()
        => CreateConnectionString();
}

public sealed record NpgsqlMultiHostConnectionSettings : NpgsqlConnectionSettings
{
    internal NpgsqlMultiHostConnectionSettings(string clientEncoding, string database, string dataSourceCached, string encoding, string kerberosServiceName, string[] hosts, int[] ports)
        : this(clientEncoding, database, dataSourceCached, encoding, kerberosServiceName, ports, hosts: hosts) { }
    
    internal NpgsqlMultiHostConnectionSettings(string clientEncoding, string database, string dataSourceCached, string encoding, string kerberosServiceName, IPAddress[] hostaddrs, int[] ports)
        : this(clientEncoding, database, dataSourceCached, encoding, kerberosServiceName, ports, hostaddrs: hostaddrs) { }
    
    internal NpgsqlMultiHostConnectionSettings(string clientEncoding, string database, string dataSourceCached, string encoding, string kerberosServiceName, string?[] hosts, IPAddress?[] hostaddrs, int[] ports)
        : this(clientEncoding, database, dataSourceCached, encoding, kerberosServiceName, ports, hosts: hosts, hostaddrs: hostaddrs) { }

    NpgsqlMultiHostConnectionSettings(string clientEncoding, string database, string dataSourceCached, string encoding, string kerberosServiceName, int[] ports, string?[]? hosts = null, IPAddress?[]? hostaddrs = null)
        : base(clientEncoding, database, dataSourceCached, encoding, kerberosServiceName)
    {
        if (ports.Length < 1)
            throw new ArgumentException("At least one port must be specified.");
        if (hosts == null && hostaddrs == null)
            throw new ArgumentNullException($"Either {nameof(hosts)} or {nameof(hostaddrs)} must be set.");

        var numberOfHosts = hosts?.Length ?? hostaddrs!.Length;
        var hasMultiplePorts = ports.Length > 1;
        if (hasMultiplePorts && ports.Length != numberOfHosts)
            throw new ArgumentException("When multiple ports are specified their number must be equal to the number of hosts.");

        var onlyPort = ports[0];
        var endpoints = ImmutableArray.CreateBuilder<EndPoint?>(numberOfHosts);
        if (hosts != null && hostaddrs != null)
        {
            if (hosts.Length != hostaddrs.Length)
                throw new ArgumentException("When both, host and hostaddr are specified they must have the same length.");

            for (var i = numberOfHosts - 1; i >= 0; i--)
            {
                var host = hosts[i];
                var hostaddr = hostaddrs[i];
                var port = hasMultiplePorts ? ports[i] : onlyPort;
                if (hostaddr != null)
                    endpoints[i] = new IPEndPoint(hostaddr, port);
                else if (host != null && IPAddress.TryParse(host, out var addr))
                    endpoints[i] = new IPEndPoint(addr, port);
                else if (host != null && NpgsqlConnectionStringBuilder.IsUnixSocket(host, port, out var socketPath))
                    endpoints[i] = new UnixDomainSocketEndPoint(socketPath);
                else if (host == null)
                    throw new InvalidOperationException(
                        $"If you specify {nameof(hosts)} and {nameof(hostaddrs)} one of them must contain a value for each index.");
            }
        }
        else if (hosts != null)
        {
            for (var i = numberOfHosts - 1; i >= 0; i--)
            {
                var host = hosts[i];
                var port = hasMultiplePorts ? ports[i] : onlyPort;
                if (host != null && IPAddress.TryParse(host, out var addr))
                    endpoints[i] = new IPEndPoint(addr, port);
                else if (host != null && NpgsqlConnectionStringBuilder.IsUnixSocket(host, port, out var socketPath))
                    endpoints[i] = new UnixDomainSocketEndPoint(socketPath);
                else if (host == null)
                    throw new InvalidOperationException(
                        $"If you only specify {nameof(hosts)} it must contain a value for each index.");
            }
        }
        else
        {
            Debug.Assert(hosts == null && hostaddrs != null);
            for (var i = numberOfHosts - 1; i >= 0; i--)
            {
                var hostaddr = hostaddrs[i];
                if (hostaddr == null)
                    throw new InvalidOperationException(
                        $"If you only specify {nameof(hostaddrs)} it must contain a value for each index.");

                var port = hasMultiplePorts ? ports[i] : onlyPort;
                endpoints[i] = new IPEndPoint(hostaddr, port);
            }
        }

        EndPoints = endpoints.MoveToImmutable();
        Hosts = ImmutableArray.Create(hosts);
        HostAddresses  = ImmutableArray.Create(hostaddrs);
        Ports = ImmutableArray.Create(ports);
        _childSettings = new NpgsqlSingleHostConnectionSettings[numberOfHosts];
    }


    /// <summary>
    /// The EndPoints to connect to
    /// </summary>
    internal ImmutableArray<EndPoint?> EndPoints { get; init; }

    /// <summary>
    /// The hosts to connect to
    /// </summary>
    public ImmutableArray<string?> Hosts { get; internal init; }

    /// <summary>
    /// The ip addresses to connect to
    /// </summary>
    public ImmutableArray<IPAddress?> HostAddresses { get; internal init; }

    /// <summary>
    /// The TCP/IP ports of the hosts to connect to.
    /// </summary>
    /// <remarks>
    /// This will be either one port that applies to all hosts
    /// or match the number of hosts, in which case each port
    /// applies to the host with the same index.
    /// </remarks>
    public ImmutableArray<int> Ports { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionSettings.MultipleHosts"/>
    internal override bool MultipleHosts => true;

    string? _safeLogHost;

    /// <inheritdoc cref="NpgsqlConnectionSettings.SafeLogHost"/>
    internal override string SafeLogHost
    {
        get
        {
            return _safeLogHost ??= GetSafeLogHost();

            string GetSafeLogHost()
            {
                var sb = new StringBuilder();
                if (HostAddresses.Length == 0)
                    foreach (var host in Hosts)
                        sb.Append(host).Append(',');
                else if (Hosts.Length == 0)
                    foreach (var hostAddress in HostAddresses)
                        sb.Append(hostAddress).Append(',');
                else
                    for (var i = 0; i < Hosts.Length; i++)
                        sb.Append(Hosts[i] ?? HostAddresses[i]!.ToString()).Append(',');

                sb.Length--;
                return sb.ToString();
            }
        }
    }

    /// <inheritdoc cref="NpgsqlConnectionSettings.SafeLogPort"/>
    internal override string SafeLogPort => Ports.Length == 1 ? Ports[0].ToString() : string.Join(",", Ports);

    public override string CreateConnectionString(PasswordHandling passwordHandling = PasswordHandling.Auto,
        DefaultHandling defaultHandling = DefaultHandling.OmitHardcodedDefaults) =>
        throw new NotImplementedException();

    readonly NpgsqlSingleHostConnectionSettings?[] _childSettings;
    internal NpgsqlSingleHostConnectionSettings GetNpgsqlSingleHostConnectionSettings(int index)
        => _childSettings[index] ??= new(this with
        {
            Hosts = ImmutableArray.Create(Hosts.Length > 0 ? Hosts[index] : null),
            HostAddresses = ImmutableArray.Create(HostAddresses.Length > 0 ? HostAddresses[index] : null),
            EndPoints = ImmutableArray.Create(EndPoints[index]),
            Ports = ImmutableArray.Create(Ports[Ports.Length > 1 ? index : 0])
        });

    public override string ToString()
        => CreateConnectionString();
}

public abstract record NpgsqlConnectionSettings
{
    internal static readonly NpgsqlConnectionSettings Default = new NpgsqlSingleHostConnectionSettings (
        NpgsqlConnectionDefaults.ClientEncoding,
        "",
        "",
        NpgsqlConnectionDefaults.Encoding,
        NpgsqlConnectionDefaults.KerberosServiceName,
        NpgsqlConnectionDefaults.Host,
        NpgsqlConnectionDefaults.Port);
    private protected NpgsqlConnectionSettings(string clientEncoding, string database, string dataSourceCached, string encoding, string kerberosServiceName)
    {
        ClientEncoding = clientEncoding;
        Database = database;
        DataSourceCached = dataSourceCached;
        Encoding = encoding;
        KerberosServiceName = kerberosServiceName;
    }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.ApplicationName"/>
    public string? ApplicationName { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.ArrayNullabilityMode"/>
    public ArrayNullabilityMode ArrayNullabilityMode { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.AutoPrepareMinUsages"/>
    public int AutoPrepareMinUsages { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.CancellationTimeout"/>
    public int CancellationTimeout { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.CheckCertificateRevocation"/>
    public bool CheckCertificateRevocation { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.ClientEncoding"/>
    public string ClientEncoding { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.CommandTimeout"/>
    public int CommandTimeout { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.ConnectionIdleLifetime"/>
    public int ConnectionIdleLifetime { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.ConnectionLifetime"/>
    public int ConnectionLifetime { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.ConnectionPruningInterval"/>
    public int ConnectionPruningInterval { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Database"/>
    public string Database { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.DataSourceCached"/>
    internal string DataSourceCached { get; init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.IncludeRealm"/>
    public bool IncludeRealm { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Encoding"/>
    public string Encoding { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Enlist"/>
    public bool Enlist { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.EntityTemplateDatabase"/>
    public string? EntityTemplateDatabase { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.EntityAdminDatabase"/>
    public string? EntityAdminDatabase { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.HostRecheckSecondsTranslated"/>
    internal TimeSpan HostRecheckSecondsTranslated { get; init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.IncludeErrorDetail"/>
    public bool IncludeErrorDetail { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.IntegratedSecurity"/>
    public bool IntegratedSecurity { get;  internal init;}

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.InternalCommandTimeout"/>
    public int InternalCommandTimeout { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.KeepAlive"/>
    public int KeepAlive { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.KerberosServiceName"/>
    public string KerberosServiceName { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.LoadBalanceHosts"/>
    public bool LoadBalanceHosts { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.LoadTableComposites"/>
    public bool LoadTableComposites { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.LogParameters"/>
    public bool LogParameters { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.MaxAutoPrepare"/>
    public int MaxAutoPrepare { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.MaxPoolSize"/>
    public int MaxPoolSize { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.MinPoolSize"/>
    public int MinPoolSize { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Multiplexing"/>
    public bool Multiplexing { get; internal init; }
    
    /// <summary>
    /// <see langword="true"/> if the settings contain multiple hosts; otherwise <see langword="false"/>
    /// </summary>
    internal abstract bool MultipleHosts { get; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.NoResetOnClose"/>
    public bool NoResetOnClose { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Options"/>
    public string? Options { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Password"/>
    public string? Password { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Passfile"/>
    public string? Passfile { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.PersistSecurityInfo"/>
    public bool PersistSecurityInfo { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Pooling"/>
    public bool Pooling { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.ReadBufferSize"/>
    public int ReadBufferSize { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.ReplicationMode"/>
    internal ReplicationMode ReplicationMode { get; init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.RootCertificate"/>
    public string? RootCertificate { get; internal init; }

    /// <summary>
    /// The host name to use for logging purposes. This may also contain multiple concatenated host names.
    /// </summary>
    internal abstract string SafeLogHost { get; }

    /// <summary>
    /// The port to use for logging purposes. This may also contain multiple concatenated ports.
    /// </summary>
    internal abstract string SafeLogPort { get; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.SearchPath"/>
    public string? SearchPath { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.ServerCompatibilityMode"/>
    public ServerCompatibilityMode ServerCompatibilityMode { get; internal init; }

    
    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.SocketReceiveBufferSize"/>
    public int SocketReceiveBufferSize { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.SocketSendBufferSize"/>
    public int SocketSendBufferSize { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.SslCertificate"/>
    public string? SslCertificate { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.SslKey"/>
    public string? SslKey { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.SslMode"/>
    public SslMode SslMode { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.SslPassword"/>
    public string? SslPassword { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.TargetSessionAttributes"/>
    public TargetSessionAttributes TargetSessionAttributes { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.TcpKeepAlive"/>
    public bool TcpKeepAlive { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.TcpKeepAliveInterval"/>
    public int TcpKeepAliveInterval { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.TcpKeepAliveTime"/>
    public int TcpKeepAliveTime { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Timeout"/>
    public int Timeout { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Timezone"/>
    public string? Timezone { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.TrustServerCertificate"/>
    public bool TrustServerCertificate { get; internal init; }

    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.Username"/>
    public string? Username { get; internal init; }
    
    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.WriteBufferSize"/>
    public int WriteBufferSize { get; internal init; }
    
    /// <inheritdoc cref="NpgsqlConnectionStringBuilder.WriteCoalescingBufferThresholdBytes"/>
    public int WriteCoalescingBufferThresholdBytes { get; internal init; }

    public abstract string CreateConnectionString(
        PasswordHandling passwordHandling = PasswordHandling.Auto,
        DefaultHandling defaultHandling = DefaultHandling.OmitHardcodedDefaults);
    
    /// <summary>
    /// Defines the password handling behavior when creating a connection string 
    /// </summary>
    public enum PasswordHandling
    {
        /// <summary>
        /// Use the <see cref="PersistSecurityInfo"/> property to decide
        /// whether the password should be included into the connection string
        /// </summary>
        Auto,

        /// <summary>
        /// Always include the password into the connection string
        /// </summary>
        Include,

        /// <summary>
        /// Never include the password into the connection string
        /// </summary>
        Exclude
    }

    /// <summary>
    /// Defines the default handling behavior when creating a connection string.
    /// </summary>
    public enum DefaultHandling
    {
        /// <summary>
        /// Omit default values that are hardcoded into Npgsql.
        /// </summary>
        OmitHardcodedDefaults,
        /// <summary>
        /// Omit default values that are constant for an operating system.
        /// </summary>
        OmitOsDefaults,
        /// <summary>
        /// Omit all default values including those that can be inferred from environment variables
        /// or other factors of the current environment (e. g. Kerberos).
        /// </summary>
        OmitAllDefaults,
        /// <summary>
        /// Include all current connection settings, even those that were inferred from the current environment.
        /// </summary>
        IncludeEverything
    }
}
