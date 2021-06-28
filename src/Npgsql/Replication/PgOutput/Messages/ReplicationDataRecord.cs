using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Internal;

#pragma warning disable 1591

namespace Npgsql.Replication.PgOutput.Messages
{
    public class ReplicationDataRecord : IDataRecord, IAsyncEnumerable<ReplicationTuple>, IEnumerable
    {
        int _currentFieldIndex;
        readonly NpgsqlReadBuffer _readBuffer;
        readonly ReplicationDataRecordEnumerator _dataRecordEnumerator;
        RelationInfo _relationInfo = default!;

        internal ReplicationDataRecord(NpgsqlReadBuffer readBuffer)
        {
            _readBuffer = readBuffer;
            _dataRecordEnumerator = new ReplicationDataRecordEnumerator(readBuffer);
        }
        
        public int FieldCount { get; private set; }

        public object this[int i]
            => GetFieldValue<object>(i);

        public object this[string name]
            => GetFieldValue<object>(_relationInfo.GetOrdinal(name));

        public bool GetBoolean(int i)
            => GetFieldValue<bool>(i);

        public byte GetByte(int i)
            => GetFieldValue<byte>(i);

        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
        {
            var stream = GetFieldValue<Stream>(i);
            return buffer == null
                ? stream.Length
                : stream.Read(buffer, bufferoffset, length);
        }

        public char GetChar(int i)
            => GetFieldValue<char>(i);

        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
            => throw new NotImplementedException();

        public IDataReader GetData(int i)
            => throw new NotImplementedException();

        public string GetDataTypeName(int i)
            => _relationInfo[i].TypeHandler.PostgresType.DisplayName;

        public DateTime GetDateTime(int i)
            => GetFieldValue<DateTime>(i);

        public decimal GetDecimal(int i)
            => GetFieldValue<decimal>(i);

        public double GetDouble(int i)
            => GetFieldValue<double>(i);

        public Type GetFieldType(int i)
            => _relationInfo[i].TypeHandler.GetFieldType();

        public float GetFloat(int i)
            => GetFieldValue<float>(i);

        public Guid GetGuid(int i)
            => GetFieldValue<Guid>(i);

        public short GetInt16(int i)
            => GetFieldValue<short>(i);

        public int GetInt32(int i)
            => GetFieldValue<int>(i);

        public long GetInt64(int i)
            => GetFieldValue<long>(i);

        public string GetName(int i)
            => _relationInfo[i].Name;

        public int GetOrdinal(string name)
            => _relationInfo.GetOrdinal(name);

        public string GetString(int i)
            => GetFieldValue<string>(i);

        public object GetValue(int i)
            => GetFieldValue<object>(i);

        public int GetValues(object[] values)
            => throw new NotImplementedException();

        public bool IsDBNull(int i)
            => throw new NotImplementedException();

        public bool IsUnchangedToastedValue(int i)
            => throw new NotImplementedException();

        public T GetFieldValue<T> (int ordinal)
            => GetFieldValue<T>(ordinal, async: false).GetAwaiter().GetResult();

        public ValueTask<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken = default)
            => GetFieldValue<T>(ordinal, async: true, cancellationToken);

        ValueTask<T> GetFieldValue<T>(int ordinal, bool async, CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
                return GetFieldValueInternal(ordinal, async, cancellationToken);

            async ValueTask<T> GetFieldValueInternal(int ordinal, bool async, CancellationToken cancellationToken)
            {
                if (ordinal >= FieldCount)
                    throw new IndexOutOfRangeException();

                if (ordinal > _currentFieldIndex)
                    _dataRecordEnumerator.Reset();

                do
                {
                    // ReSharper disable once MethodHasAsyncOverload
                    if (!(async ? await _dataRecordEnumerator.MoveNextAsync() : _dataRecordEnumerator.MoveNext()))
                        throw new InvalidOperationException(
                            $"You can only access unbuffered {nameof(ReplicationDataRecord)} tuples sequentially.");
                } while (ordinal < _currentFieldIndex++);

                Debug.Assert(_dataRecordEnumerator.Current != null);
                if (ordinal + 1 == _currentFieldIndex)
                {
                    return await _dataRecordEnumerator.Current.GetValueAsync<T>(cancellationToken);
                }

                throw new InvalidOperationException("You can only access NpgsqlDataRecord tuples sequentially.");
            }
        }

        public IEnumerator<ReplicationTuple> GetEnumerator()
            => _dataRecordEnumerator;

        IEnumerator IEnumerable.GetEnumerator()
            => _dataRecordEnumerator;

        public IAsyncEnumerator<ReplicationTuple> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            => _dataRecordEnumerator.SetCancellationToken(cancellationToken);

        // ToDo: DRY
        internal ReplicationDataRecord Clone()
        {
            if (!_dataRecordEnumerator.TryReset())
                throw new InvalidOperationException($"Cloning a {nameof(ReplicationDataRecord)} is not supported after starting to read its fields.");

            var buffer = new MemoryStream();
            using var enumerator = _dataRecordEnumerator;
            while (enumerator.MoveNext())
            {
                Debug.Assert(enumerator.Current != null);
                if (enumerator.Current.IsDBNull)
                    buffer.WriteByte((byte)TupleDataKind.Null);
                else if (enumerator.Current.IsUnchangedToastedValue)
                    buffer.WriteByte((byte)TupleDataKind.UnchangedToastedValue);
                else
                {
                    if (enumerator.Current.IsTextValue)
                        buffer.WriteByte((byte)TupleDataKind.TextValue);
                    else if (enumerator.Current.IsBinaryValue)
                        buffer.WriteByte((byte)TupleDataKind.BinaryValue);

                    buffer.Write(BitConverter.GetBytes(BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(enumerator.Current.Length) : enumerator.Current.Length));
                    enumerator.Current.GetStream().CopyTo(buffer);
                }
            }

            buffer.Position = 0;
            var readBuffer = new NpgsqlReadBuffer(_readBuffer.Connector, buffer, null, MinBufferLength(buffer.Length),
                _readBuffer.Connector.TextEncoding, _readBuffer.Connector.RelaxedTextEncoding, usePool: true);

            return new ReplicationDataRecord(readBuffer).Init((ushort)FieldCount, _relationInfo);
        }

        internal ValueTask<ReplicationDataRecord> CloneAsync(CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
                return CloneAsyncInternal(cancellationToken);

            async ValueTask<ReplicationDataRecord> CloneAsyncInternal(CancellationToken cancellationToken)
            {
                if (!_dataRecordEnumerator.TryReset())
                    throw new InvalidOperationException($"Cloning a {nameof(ReplicationDataRecord)} is not supported after starting to read its fields.");

                var buffer = new MemoryStream();
                await using var enumerator = GetAsyncEnumerator(cancellationToken);
                while (await enumerator.MoveNextAsync())
                {
                    Debug.Assert(enumerator.Current != null);
                    if (enumerator.Current.IsDBNull)
                        buffer.WriteByte((byte)TupleDataKind.Null);
                    else if (enumerator.Current.IsUnchangedToastedValue)
                        buffer.WriteByte((byte)TupleDataKind.UnchangedToastedValue);
                    else
                    {
                        if (enumerator.Current.IsTextValue)
                            buffer.WriteByte((byte)TupleDataKind.TextValue);
                        else if (enumerator.Current.IsBinaryValue)
                            buffer.WriteByte((byte)TupleDataKind.BinaryValue);

                        buffer.Write(BitConverter.GetBytes(BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(enumerator.Current.Length) : enumerator.Current.Length));
                        await enumerator.Current.GetStream().CopyToAsync(buffer, 8192, cancellationToken);
                    }
                }

                buffer.Position = 0;
                // Hack: Abuse a NpgsqlReadBuffer as buffer. This currently costs at least 4096 bytes per row!
                var readBuffer = new NpgsqlReadBuffer(_readBuffer.Connector, buffer, null, MinBufferLength(buffer.Length),
                    _readBuffer.Connector.TextEncoding, _readBuffer.Connector.RelaxedTextEncoding, usePool: true);

                return new ReplicationDataRecord(readBuffer).Init((ushort)FieldCount, _relationInfo);
            }
        }

        static int MinBufferLength(long actualBufferLength)
            => actualBufferLength > NpgsqlReadBuffer.MinimumSize
                ? checked((int)actualBufferLength)
                : NpgsqlReadBuffer.MinimumSize;

        internal ReplicationDataRecord Init(ushort fieldCount, RelationInfo relationInfo)
        {
            // Hack: Set AttemptPostgresCancellation back to true on the connector in case it has been left at false (e. g. by GetStream())
            _readBuffer.Connector.StartNestedCancellableOperation().Dispose();

            FieldCount = fieldCount;
            _relationInfo = relationInfo;
            _dataRecordEnumerator.Init(fieldCount, relationInfo);
            _currentFieldIndex = 0;
            return this;
        }

        internal async Task Cleanup()
            => await _dataRecordEnumerator.DisposeAsync();
    }
}
