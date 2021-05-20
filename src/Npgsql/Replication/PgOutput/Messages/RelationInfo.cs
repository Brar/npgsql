using System.Collections.Generic;
using Npgsql.Internal.TypeHandling;
using Npgsql.TypeMapping;

namespace Npgsql.Replication.PgOutput.Messages
{
    class RelationInfo
    {
        readonly Dictionary<string, int> _nameIndices
            = new(/* ToDo: Comparer */);
        ColumnInfo[] _columnInfos;

        public RelationInfo(RelationMessage msg, ConnectorTypeMapper typeMapper)
        {
            Id = msg.RelationId;
            Namespace = msg.Namespace;
            Name = msg.RelationName;
            var len = msg.Columns.Count;
            _columnInfos = new ColumnInfo[len];
            for (var index = 0; index < len; index++)
            {
                var columnInfo = new ColumnInfo(msg.Columns[index], typeMapper);
                _columnInfos[index] = columnInfo;
                _nameIndices.Add(columnInfo.Name, index);
            }
        }

        public uint Id { get; private set; }
        public string Namespace { get; private set; }
        public string Name { get; private set; }
        public ColumnInfo this[int id] => _columnInfos[id];
        public ColumnInfo this[string name] => _columnInfos[_nameIndices[name]];

        public int GetOrdinal(string name)
            => _nameIndices[name];

        /// <summary>
        /// Updates information while trying to avoid allocations and be efficient
        /// when there's nothing to actually update
        /// </summary>
        public void Update(RelationMessage msg, ConnectorTypeMapper typeMapper)
        {
            Id = msg.RelationId;
            Namespace = msg.Namespace;
            Name = msg.RelationName;
            var len = msg.Columns.Count;
            if (len != _columnInfos.Length)
            {
                RegenerateCollections(new ColumnInfo[len], msg, typeMapper);
                return;
            }

            for (var index = 0; index < len; index++)
            {
                if (!_columnInfos[index].Update(msg.Columns[index], typeMapper))
                    continue;

                RegenerateCollections(_columnInfos, msg, typeMapper);
                return;
            }

            void RegenerateCollections(ColumnInfo[] columnInfos, RelationMessage message, ConnectorTypeMapper mapper)
            {
                _columnInfos = columnInfos;
                _nameIndices.Clear();
                for (var index = 0; index < columnInfos.Length; index++)
                {
                    var columnInfo = new ColumnInfo(message.Columns[index], mapper);
                    _columnInfos[index] = columnInfo;
                    _nameIndices.Add(columnInfo.Name, index);
                }
            }
        }
    }

    class ColumnInfo
    {
        public ColumnInfo(RelationMessage.Column column, ConnectorTypeMapper typeMapper)
        {
            DataTypeId = column.DataTypeId;
            Name = column.ColumnName;
            TypeModifier = column.TypeModifier;
            TypeHandler = typeMapper.GetByOID(DataTypeId);
        }

        public uint DataTypeId { get; private set; }
        public int TypeModifier { get; private set; }
        public string Name { get; private set; }
        public NpgsqlTypeHandler TypeHandler { get; private set; }

        /// <summary>
        /// Updates internal information trying to be efficient
        /// </summary>
        /// <returns>
        /// <see langword="true"/> if the column name has been changed; otherwise <see langword="false"/>
        /// </returns>
        public bool Update(RelationMessage.Column column, ConnectorTypeMapper typeMapper)
        {
            if (column.DataTypeId != DataTypeId)
            {
                DataTypeId = column.DataTypeId;
                TypeHandler = typeMapper.GetByOID(DataTypeId);
            }

            TypeModifier = column.TypeModifier;

            if (column.ColumnName == Name)
                return false;

            Name = column.ColumnName;
            return true;

        }
    }
}
