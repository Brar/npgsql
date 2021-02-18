namespace Npgsql.Replication
{
    internal readonly struct PgTableSpaceInfo
    {
        internal PgTableSpaceInfo(uint? oid, string? path, ulong? size)
        {
            Oid = oid;
            Path = path;
            Size = size;
        }

        public uint? Oid { get; }

        public string? Path { get; }

        public ulong? Size { get; }
    }
}
