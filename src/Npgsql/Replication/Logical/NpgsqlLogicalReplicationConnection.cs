using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.Logging;
using Npgsql.Replication.Logical.Protocol;
using Npgsql.TypeHandlers.DateTimeHandlers;

namespace Npgsql.Replication.Logical
{
    /// <summary>
    /// Represents a logical replication connection to a PostgreSQL server
    /// </summary>
    public sealed class NpgsqlLogicalReplicationConnection : NpgsqlReplicationConnection
    {
        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(NpgsqlLogicalReplicationConnection));

        /// <summary>
        /// Initializes a new instance of <see cref="NpgsqlLogicalReplicationConnection"/>.
        /// </summary>
        public NpgsqlLogicalReplicationConnection() {}

        /// <summary>
        /// Initializes a new instance of <see cref="NpgsqlLogicalReplicationConnection"/>.
        /// </summary>
        public NpgsqlLogicalReplicationConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        #region Open

        /// <summary>
        /// Opens a database replication connection with the property settings specified by the
        /// <see cref="NpgsqlReplicationConnection.ConnectionString">ConnectionString</see>.
        /// </summary>
        [PublicAPI]
        public override Task OpenAsync(CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
            {
                return OpenAsync(new NpgsqlConnectionStringBuilder(ConnectionString)
                {
                    ReplicationMode = ReplicationMode.Logical
                }, cancellationToken);
            }
        }

        #endregion Open
    }
}
