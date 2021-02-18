using System;
using System.Collections.Generic;
using System.Text;

namespace Npgsql.Replication
{
    /// <summary>
    /// 
    /// </summary>
    public interface IBackupResponse
    {
        /// <summary>
        /// 
        /// </summary>
        public BackupResponseKind Kind { get; }
    }
}
