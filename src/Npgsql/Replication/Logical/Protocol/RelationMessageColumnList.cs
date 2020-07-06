using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using JetBrains.Annotations;

namespace Npgsql.Replication.Logical.Protocol
{
    class RelationMessageColumnList : IReadOnlyList<RelationMessageColumn>
    {
        internal List<RelationMessageColumn> InternalList { get; }

        public RelationMessageColumnList(int numberOfColumns)
            => InternalList = new List<RelationMessageColumn>(numberOfColumns);

        public IEnumerator<RelationMessageColumn> GetEnumerator() => InternalList.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable)InternalList).GetEnumerator();

        public int Count => InternalList.Count;

        public RelationMessageColumn this[int index] => InternalList[index];
    }
}
