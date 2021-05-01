namespace Npgsql.Replication
{
    /// <summary>
    /// 
    /// </summary>
    public sealed class TableSpaceInfoMessage : IBackupResponse
    {
        internal TableSpaceInfoMessage(uint? oid, string? path, ulong? size)
        {
            Oid = oid;
            Path = path;
            Size = size;
        }

        /// <summary>
        /// 
        /// </summary>
        public uint? Oid { get; }

        /// <summary>
        /// 
        /// </summary>
        public string? Path { get; }

        /// <summary>
        /// 
        /// </summary>
        public ulong? Size { get; }

        BackupResponseKind IBackupResponse.Kind => BackupResponseKind.TablespaceInfoMessage;
    }
}
