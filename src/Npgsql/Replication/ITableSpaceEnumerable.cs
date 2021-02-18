using System;
using System.Collections.Generic;

namespace Npgsql.Replication
{
    /// <summary>
    /// 
    /// </summary>
    public interface ITableSpaceEnumerable : IBackupResponse, IAsyncEnumerable<PgTarFileStream>, IAsyncDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        uint? Oid { get; }

        /// <summary>
        /// 
        /// </summary>
        string? Path { get; }

        /// <summary>
        /// 
        /// </summary>
        ulong? ApproximateSize { get; }
    }
}
