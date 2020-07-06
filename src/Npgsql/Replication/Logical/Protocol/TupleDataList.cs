using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using JetBrains.Annotations;

namespace Npgsql.Replication.Logical.Protocol
{
    class TupleDataList : IReadOnlyList<TupleData>
    {
        internal IList<TupleData> InternalList { get; }

        public TupleDataList(int numberOfColumns)
            => InternalList = new List<TupleData>(numberOfColumns);

        public IEnumerator<TupleData> GetEnumerator()
            => InternalList.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => ((IEnumerable)InternalList).GetEnumerator();

        public int Count
            => InternalList.Count;

        public TupleData this[int index]
            => InternalList[index];
    }
}
