using System;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql;

record NpgsqlDataSourceConfiguration(
    NpgsqlLoggingConfiguration LoggingConfiguration,
    Func<NpgsqlConnectionSettings, CancellationToken, ValueTask<string>>? PeriodicPasswordProvider,
    TimeSpan PeriodicPasswordSuccessRefreshInterval,
    TimeSpan PeriodicPasswordFailureRefreshInterval);
