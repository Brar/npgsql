namespace Npgsql.Replication
{
    /// <summary>
    /// 
    /// </summary>
    public enum BackupResponseKind
    {
        /// <summary>
        /// 
        /// </summary>
        StartMessage,
        /// <summary>
        /// 
        /// </summary>
        TablespaceMessage,
        /// <summary>
        /// 
        /// </summary>
        ManifestMessage,
        /// <summary>
        /// 
        /// </summary>
        EndMessage,
    }
}
