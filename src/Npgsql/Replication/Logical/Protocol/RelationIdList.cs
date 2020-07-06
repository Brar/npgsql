using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Npgsql.Replication.Logical.Protocol
{
    class RelationIdList : IReadOnlyList<uint>
    {
        internal List<uint> InternalList { get; }

        public RelationIdList(int numberOfColumns)
            => InternalList = new List<uint>(numberOfColumns);

        public IEnumerator<uint> GetEnumerator() => InternalList.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable)InternalList).GetEnumerator();

        public int Count => InternalList.Count;

        public uint this[int index] => InternalList[index];
    }
}
