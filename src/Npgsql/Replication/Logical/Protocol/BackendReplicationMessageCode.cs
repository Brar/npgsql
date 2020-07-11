namespace Npgsql.Replication.Logical.Protocol
{
    enum BackendReplicationMessageCode : byte
    {
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
