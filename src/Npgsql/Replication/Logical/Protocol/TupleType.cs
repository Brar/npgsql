namespace Npgsql.Replication.Logical.Protocol
{
    enum TupleType : byte
    {
        Key = (byte)'K',
        NewTuple = (byte)'N',
        OldTuple = (byte)'O',
    }
}
