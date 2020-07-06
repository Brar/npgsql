namespace Npgsql.Replication.Logical
{
    enum BackendReplicationMessageCode : byte
    {
        Invalid = 0,
        Begin = (byte)'B',
        Commit = (byte)'C',
        Origin = (byte)'O',
        Relation = (byte)'R',
        Type = (byte)'Y',
        Insert = (byte)'I',
        Update = (byte)'U',
        Delete = (byte)'D',
        Truncate = (byte)'T'
    }
}
