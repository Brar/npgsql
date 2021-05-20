using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Internal;

namespace Npgsql.Replication.PgOutput.Messages
{
    sealed class ReplicationDataRecordEnumerator : IAsyncEnumerator<ReplicationTuple>, IEnumerator<ReplicationTuple>
    {
        readonly NpgsqlReadBuffer _readBuffer;
        readonly ReplicationTuple _current = new();
        bool _disposed;
        CancellationToken _cancellationToken;
        int _currentFieldIndex;
        int _fieldCount;
        RelationInfo _relationInfo = default!;

        internal ReplicationDataRecordEnumerator(NpgsqlReadBuffer readBuffer)
            => _readBuffer = readBuffer;

        // Hack: Detect buffering by looking at the underlying stream
        // ReSharper disable once ConditionIsAlwaysTrueOrFalse
        bool IsBuffered => _readBuffer.Underlying is MemoryStream;

        public ReplicationTuple Current => _disposed
            ? throw new ObjectDisposedException(nameof(ReplicationDataRecordEnumerator))
            : _current;

        object IEnumerator.Current => Current;

        public ValueTask<bool> MoveNextAsync()
            => MoveNext(async: true);

        public bool MoveNext()
            => MoveNext(async: false).GetAwaiter().GetResult();

        ValueTask<bool> MoveNext(bool async)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ReplicationDataRecordEnumerator));

            using (NoSynchronizationContextScope.Enter())
                return MoveNextInternal(async);

            async ValueTask<bool> MoveNextInternal(bool async)
            {
                if (_currentFieldIndex > 0)
                    await Current.Cleanup(async, _cancellationToken);

                if (_currentFieldIndex >= _fieldCount)
                    return false;

                using var tokenRegistration = IsBuffered
                    ? default
                    : _readBuffer.Connector.StartNestedCancellableOperation(_cancellationToken);

                await _readBuffer.Ensure(1, async);
                var kind = (TupleDataKind)_readBuffer.ReadByte();
                switch (kind)
                {
                case TupleDataKind.Null:
                case TupleDataKind.UnchangedToastedValue:
                    Current.Init(_readBuffer, 0, kind, _relationInfo[_currentFieldIndex].TypeHandler);
                    break;
                case TupleDataKind.TextValue:
                case TupleDataKind.BinaryValue:
                    await _readBuffer.Ensure(4, async);
                    var len = _readBuffer.ReadInt32();
                    Current.Init(_readBuffer, len, kind, _relationInfo[_currentFieldIndex].TypeHandler);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
                }

                _currentFieldIndex++;
                return true;
            }
        }

        public void Reset()
        {
            if (!TryReset())
                throw new InvalidOperationException("Resetting streaming enumerators is not supported.");
        }

        internal bool TryReset()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ReplicationDataRecordEnumerator));
            if (!IsBuffered && _currentFieldIndex > 0)
                return false;
            _currentFieldIndex = 0;
            return true;
        }

        public void Dispose()
        {
            if (!_disposed) while (MoveNext()) { /* Do nothing, just iterate the enumerator */ }
            _disposed = true;
        }

        public async ValueTask DisposeAsync()
        {
            if (!_disposed) while (await MoveNextAsync()) { /* Do nothing, just iterate the enumerator */ }
            _disposed = true;
        }

        internal void Init(int fieldCount, RelationInfo relationInfo)
        {
            _disposed = false;
            _currentFieldIndex = 0;
            _fieldCount = fieldCount;
            _relationInfo = relationInfo;
        }

        internal ReplicationDataRecordEnumerator SetCancellationToken(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            return this;
        }
    }
}
