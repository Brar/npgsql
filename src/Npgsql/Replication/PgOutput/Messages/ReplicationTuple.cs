using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Internal;
using Npgsql.Internal.TypeHandling;

#pragma warning disable 1591

namespace Npgsql.Replication.PgOutput.Messages
{
    public sealed class ReplicationTuple
    {
        internal int Length { get; private set; }
        TupleDataKind _tupleDataKind;
        NpgsqlReadBuffer _readBuffer = default!;
        NpgsqlTypeHandler _typeHandler = default!;
        Stream? _stream;
        bool _isRead;

        // ReSharper disable once InconsistentNaming
        public bool IsDBNull => _tupleDataKind == TupleDataKind.Null;

        public bool IsUnchangedToastedValue => _tupleDataKind == TupleDataKind.UnchangedToastedValue;

        public bool IsBinaryValue => _tupleDataKind == TupleDataKind.BinaryValue;

        public bool IsTextValue => _tupleDataKind == TupleDataKind.TextValue;

        bool IsBuffered => _readBuffer.Underlying is MemoryStream;

        bool IsRead => _isRead || Length < 1;

        public T GetValue<T>()
            => GetValue<T>(async: false).GetAwaiter().GetResult();

        public ValueTask<T> GetValueAsync<T>(CancellationToken cancellationToken = default)
            => GetValue<T>(async: true, cancellationToken);

        ValueTask<T> GetValue<T>(bool async, CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
                return GetValueInternal(async, cancellationToken);

            async ValueTask<T> GetValueInternal(bool async, CancellationToken cancellationToken)
            {
                var position = _readBuffer.ReadPosition;
                try
                {
                    if (IsRead)
                    {
                        if (IsBuffered)
                            _readBuffer.ReadPosition -= Length;
                        else
                            throw new NpgsqlException("You can't read an unbuffered value twice.");
                    }

                    switch (_tupleDataKind)
                    {
                    case TupleDataKind.TextValue:
                    {
                        if (typeof(T).IsAssignableFrom(typeof(Stream)))
                            return (T)(object)GetStreamInternal();

                        if (typeof(T) != typeof(object) && !typeof(T).IsAssignableFrom(typeof(string)))
                            throw new NotSupportedException(
                                "Npgsql does not support converting tuple data in text format to types that are not assignable from string.");

                        using var tokenRegistration = IsBuffered
                            ? default
                            : _readBuffer.Connector.StartNestedCancellableOperation(cancellationToken);
                        await _readBuffer.Ensure(Length, async);

                        _isRead = true;
                        return (T)(object)_readBuffer.ReadString(Length);
                    }
                    case TupleDataKind.BinaryValue:
                    {
                        if (typeof(T).IsAssignableFrom(typeof(Stream)))
                            return (T)(object)GetStreamInternal();

                        using var tokenRegistration = IsBuffered
                            ? default
                            : _readBuffer.Connector.StartNestedCancellableOperation(cancellationToken);
                        await _readBuffer.Ensure(Length, async);

                        _isRead = true;
                        return NullableHandler<T>.Exists
                            ? NullableHandler<T>.Read(_typeHandler, _readBuffer, Length)
                            : typeof(T) == typeof(object)
                                ? (T)_typeHandler.ReadAsObject(_readBuffer, Length)
                                : _typeHandler.Read<T>(_readBuffer, Length);
                    }
                    case TupleDataKind.Null:
                    {
                        if (NullableHandler<T>.Exists)
                            return default!;

                        if (typeof(T) == typeof(object) || typeof(T) == typeof(DBNull))
                            return (T)(object)DBNull.Value;

                        throw new InvalidOperationException($"You can not convert {nameof(DBNull)} to {nameof(T)}.");
                    }
                    case TupleDataKind.UnchangedToastedValue:
                        throw new InvalidOperationException("You can not access an unchanged toasted value.");
                    default:
                        throw new NpgsqlException(
                            $"Unexpected {nameof(TupleDataKind)} with value '{_tupleDataKind}'. Please report this as bug.");
                    }
                }
                catch
                {
                    if (_readBuffer.Connector.State != ConnectorState.Broken)
                    {
                        var writtenBytes = _readBuffer.ReadPosition - position;
                        var remainingBytes = Length - writtenBytes;
                        if (remainingBytes > 0)
                            await _readBuffer.Skip(remainingBytes, async);
                    }

                    throw;
                }
            }
        }

        public Stream GetStream()
        {
            switch (_tupleDataKind)
            {
            case TupleDataKind.TextValue:
            case TupleDataKind.BinaryValue:
                return GetStreamInternal();
            case TupleDataKind.Null:
                throw new InvalidOperationException($"You can not read {nameof(DBNull)} as {nameof(Stream)}.");
            case TupleDataKind.UnchangedToastedValue:
                throw new InvalidOperationException("You can not access an unchanged toasted value.");
            default:
                throw new NpgsqlException($"Unexpected {nameof(TupleDataKind)} with value '{_tupleDataKind}'. Please report this as bug.");
            }
        }

        Stream GetStreamInternal()
            => _stream ??= _readBuffer.GetStream(Length, IsBuffered);

        internal void Init(NpgsqlReadBuffer readBuffer, int length, TupleDataKind kind, NpgsqlTypeHandler npgsqlTypeHandler)
        {
            _isRead = false;
            _readBuffer = readBuffer;
            Length = length;
            _tupleDataKind = kind;
            _typeHandler = npgsqlTypeHandler;
        }

        internal async Task Cleanup(bool async, CancellationToken cancellationToken = default)
        {
            // Always dispose the stream if one has been requested to signal that it is
            // no longer usable
            if (_stream != null)
            {
#if NETSTANDARD2_0
                    _stream.Dispose();
#else
                if (async)
                    await _stream.DisposeAsync();
                else
                    // ReSharper disable once MethodHasAsyncOverload
                    _stream.Dispose();
#endif
                _stream = null;
            }
            else if (!_isRead)
            {
                using var tokenRegistration = IsBuffered
                    ? default
                    : _readBuffer.Connector.StartNestedCancellableOperation(cancellationToken);
                await _readBuffer.Skip(Length, async);
            }

            _isRead = true;
        }
    }
}
