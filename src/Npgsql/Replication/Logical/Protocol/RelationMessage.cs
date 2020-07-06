using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Logical Replication Protocol relation message
    /// </summary>
    public sealed class RelationMessage : LogicalReplicationProtocolMessage
    {
        internal RelationMessage(LogSequenceNumber walStart, LogSequenceNumber walEnd, DateTime serverClock,
            uint relationId, string ns, string relationName, char relationReplicaIdentitySetting,
            RelationMessageColumnList columns) : base(walStart, walEnd, serverClock)
        {
            RelationId = relationId;
            Namespace = ns;
            RelationName = relationName;
            RelationReplicaIdentitySetting = relationReplicaIdentitySetting;
            Columns = columns;
        }

        /// <summary>
        /// ID of the relation.
        /// </summary>
        public uint RelationId { get; }

        /// <summary>
        /// Namespace (empty string for pg_catalog).
        /// </summary>
        public string Namespace { get; }

        /// <summary>
        /// Relation name.
        /// </summary>
        public string RelationName { get; }

        /// <summary>
        /// Replica identity setting for the relation (same as relreplident in pg_class).
        /// </summary>
        public char RelationReplicaIdentitySetting { get; }

        /// <summary>
        /// Relation columns
        /// </summary>
        public IReadOnlyList<RelationMessageColumn> Columns { get; }
    }
}
