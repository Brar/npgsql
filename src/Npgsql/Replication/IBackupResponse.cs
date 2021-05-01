namespace Npgsql.Replication
{
    /// <summary>
    /// Base interface of all base backup response messages.
    /// </summary>
    public interface IBackupResponse
    {
        /// <summary>
        /// The kind of message
        /// </summary>
        public BackupResponseKind Kind { get; }
    }
}
