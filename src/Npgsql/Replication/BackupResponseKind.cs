namespace Npgsql.Replication
{
    /// <summary>
    /// Describes the kind of response message returned by <see cref="ReplicationConnection.BaseBackup"/>
    /// </summary>
    public enum BackupResponseKind
    {
        /// <summary>
        /// A <see cref="BackupPositionMessage"/> containing the WAL starting position of the backup.
        /// </summary>
        StartMessage,
        /// <summary>
        /// A <see cref="TableSpaceInfoMessage"/> which can be used to retrieve information about a tablespace.
        /// </summary>
        TablespaceInfoMessage,
        /// <summary>
        /// A <see cref="TablespaceBackupTarStream"/> which can be used to retrieve tablespace data.
        /// </summary>
        TablespaceDataMessage,
        /// <summary>
        /// 
        /// </summary>
        ManifestMessage,
        /// <summary>
        /// A <see cref="BackupPositionMessage"/> containing the WAL end position of the backup.
        /// </summary>
        EndMessage,
    }
}
