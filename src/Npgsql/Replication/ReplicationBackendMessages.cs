using System.Collections.Generic;

#pragma warning disable 1591

namespace Npgsql.Replication
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

    // TODO: Base class or interface? We already have IBackendMessage...
    public class OutputReplicationMessage {}

    public sealed class BeginMessage : OutputReplicationMessage
    {
        /// <summary>
        /// The final LSN of the transaction.
        /// </summary>
        public ulong TransactionFinalLsn { get; set; }

        /// <summary>
        /// Commit timestamp of the transaction.
        /// The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
        /// </summary>
        public ulong TransactionCommitTimestamp { get; set; }

        /// <summary>
        /// Xid of the transaction.
        /// </summary>
        public uint TransactionXid { get; set; }
    }

    public sealed class CommitMessage : OutputReplicationMessage
    {
        /// <summary>
        /// Xid of the transaction.
        /// </summary>
        public uint Flags { get; set; }

        /// <summary>
        /// The LSN of the commit.
        /// </summary>
        public ulong CommitLsn { get; set; }

        /// <summary>
        /// The end LSN of the transaction.
        /// </summary>
        public ulong TransactionEndLsn { get; set; }

        /// <summary>
        /// Commit timestamp of the transaction.
        /// The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
        /// </summary>
        public ulong TransactionCommitTimestamp { get; set; }
    }

    public sealed class OriginMessage : OutputReplicationMessage
    {
        /// <summary>
        /// The LSN of the commit on the origin server.
        /// </summary>
        public ulong OriginCommitLsn { get; set; }

        /// <summary>
        /// Name of the origin.
        /// </summary>
        public string OriginName { get; set; } = string.Empty;
    }

    public sealed class RelationMessage : OutputReplicationMessage
    {
        /// <summary>
        /// ID of the relation.
        /// </summary>
        public uint RelationId { get; set; }

        /// <summary>
        /// Namespace (empty string for pg_catalog).
        /// </summary>
        public string Namespace { get; set; } = string.Empty;

        /// <summary>
        /// Relation name.
        /// </summary>
        public string RelationName { get; set; } = string.Empty;

        /// <summary>
        /// Replica identity setting for the relation (same as relreplident in pg_class).
        /// </summary>
        public byte RelationReplicaIdentitySetting { get; set; }

        /// <summary>
        /// Number of columns.
        /// </summary>
        public ushort NumColumns { get; set; }

        List<RelationColumn> Columns { get; } = new List<RelationColumn>();

        // TODO: Inner?
        public class RelationColumn
        {
            /// <summary>
            /// Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key.
            /// </summary>
            public byte Flags { get; set; }

            /// <summary>
            /// Name of the column.
            /// </summary>
            public string ColumnName { get; set; } = string.Empty;

            /// <summary>
            /// ID of the column's data type.
            /// </summary>
            public uint DataTypeId { get; set; }

            /// <summary>
            /// Type modifier of the column (atttypmod).
            /// </summary>
            public uint TypeModifier { get; set; }
        }
    }

    public sealed class TypeMessage : OutputReplicationMessage
    {
        /// <summary>
        /// ID of the data type.
        /// </summary>
        public uint TypeId { get; set; }

        /// <summary>
        /// Namespace (empty string for pg_catalog).
        /// </summary>
        public string Namespace { get; set; } = string.Empty;

        /// <summary>
        /// Name of the data type.
        /// </summary>
        public string Name { get; set; } = string.Empty;
    }

    public sealed class InsertMessage : OutputReplicationMessage
    {
        /// <summary>
        /// ID of the relation corresponding to the ID in the relation message.
        /// </summary>
        public uint RelationId { get; set; }

        // TODO: Byte1('N'): Identifies the following TupleData message as a new tuple.

        /// <summary>
        /// Columns representing the new row.
        /// </summary>
        public List<Column> NewRow { get; set; } = new List<Column>();
    }

    public sealed class UpdateMessage : OutputReplicationMessage
    {
        /// <summary>
        /// ID of the relation corresponding to the ID in the relation message.
        /// </summary>
        public uint RelationId { get; set; }

        // TODO: Byte1('N'): Identifies the following TupleData message as a new tuple.

        /// <summary>
        /// Columns representing the primary key.
        /// </summary>
        public List<Column>? KeyRow { get; set; } = new List<Column>();

        /// <summary>
        /// Columns representing the old values.
        /// </summary>
        public List<Column>? OldRow { get; set; } = new List<Column>();

        /// <summary>
        /// Columns representing the new row.
        /// </summary>
        public List<Column> NewRow { get; set; } = new List<Column>();
    }

    public sealed class DeleteMessage : OutputReplicationMessage
    {
        /// <summary>
        /// ID of the relation corresponding to the ID in the relation message.
        /// </summary>
        public uint RelationId { get; set; }

        // TODO: Byte1('N'): Identifies the following TupleData message as a new tuple.

        /// <summary>
        /// Columns representing the primary key.
        /// </summary>
        public List<Column>? KeyRow { get; set; } = new List<Column>();

        /// <summary>
        /// Columns representing the old values.
        /// </summary>
        public List<Column>? OldRow { get; set; } = new List<Column>();
    }

    public sealed class TruncateMessage : OutputReplicationMessage
    {
        /// <summary>
        /// Number of relations.
        /// </summary>
        public uint NumRelations { get; set; }

        /// <summary>
        /// Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY.
        /// </summary>
        public byte Options { get; set; }

        /// <summary>
        /// ID of the relation corresponding to the ID in the relation message. This field is repeated for each relation.
        /// </summary>
        public uint RelationId { get; set; }
    }

    public struct Column
    {
        public ColumnValueType Type { get; set; }

        /// <summary>
        /// The value of the column, in text format, if <see cref="Type" /> is <see cref="ColumnValueType.TextValue"/>.
        /// Otherwise <see langword="null" />.
        /// </summary>
        public string Value { get; set; }
    }

    public enum ColumnValueType
    {
        /// <summary>
        /// Identifies the data as NULL value.
        /// </summary>
        IsNull,

        /// <summary>
        /// Identifies unchanged TOASTed value (the actual value is not sent).
        /// </summary>
        UnchangedToastedValue,

        /// <summary>
        /// Identifies the data as text formatted value.
        /// </summary>
        TextValue
    }
}
