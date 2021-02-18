using System;
using System.Collections.Generic;
using System.Text;
using NpgsqlTypes;

namespace Npgsql.Replication
{
    /// <summary>
    /// 
    /// </summary>
    public class BackupPositionMessage : IBackupResponse
    {

        internal BackupPositionMessage(BackupResponseKind kind, NpgsqlLogSequenceNumber position, uint timelineId)
        {
            Kind = kind;
            Position = position;
            TimelineId = timelineId;
        }

        /// <summary>
        /// 
        /// </summary>
        public BackupResponseKind Kind { get; }

        /// <summary>
        /// 
        /// </summary>
        public NpgsqlLogSequenceNumber Position { get; }

        /// <summary>
        /// 
        /// </summary>
        public uint TimelineId { get; }
    }
}
