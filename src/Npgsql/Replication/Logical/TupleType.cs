namespace Npgsql.Replication.Logical
{
    enum TupleType : byte
    {
        Key = (byte)'K',
        NewTuple = (byte)'N',
        OldTuple = (byte)'O',
    }
}
