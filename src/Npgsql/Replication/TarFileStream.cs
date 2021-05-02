using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.Util;
#if !NETSTANDARD2_0
// Hack to use UnixEpoch from .NET if available
using static System.DateTime;
#endif


namespace Npgsql.Replication
{
    /// <summary>
    /// A read only <see cref="Stream"/> wrapping a PostgreSQl base backup file in USTAR format
    /// </summary>
    public class TarFileStream : Stream
    {
#if NETSTANDARD2_0
        static readonly DateTime UnixEpoch = new (1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
#endif
        internal static TarFileStream Instance = new();

        NpgsqlConnector _connector = default!;
        CancellationToken _baseCancellationToken;
        int _length; // The length of the tar file content
        int _totalLength; // The length of the tar file content including padding
        int _currentMessageLength; // The length of the current CopyDataMessage
        int _read; // The total number of bytes read
        int _currentMessageRead; // The number of bytes read from the current CopyDataMessage

        TarFileStream() { }

        internal async Task<TarFileStream> Load(NpgsqlConnector connector, CancellationToken baseCancellationToken)
        {
            _connector = connector;
            _baseCancellationToken = baseCancellationToken;
            _read = 0;
            _currentMessageRead = 0;
            _currentMessageLength = 0;
            var buf = _connector.ReadBuffer;
            await buf.Ensure(ReplicationConnection.TarBlockSize, async: true);
            var name = ReadAsciiString(100);
            Mode = ReadOctalString(8);
            UId = ReadOctalString(8);
            GId = ReadOctalString(8);
            _length = ReadOctalString(12);
            _totalLength = _length + TarPaddingBytesRequired(_length);
            var mtime = ReadOctalString(12);
            MTime = UnixEpoch.AddSeconds(mtime);
            ChkSum = ReadOctalString(8);
            TypeFlag = (char)buf.ReadByte();
            LinkName = ReadAsciiString(100);
            // UStar indicator and version
            buf.Skip(8);
            UName = ReadAsciiString(32);
            GName = ReadAsciiString(32);
            DevMajor = ReadOctalString(8);
            DevMinor = ReadOctalString(8);
            var prefix = ReadAsciiString(155);
            Name = prefix.Length > 0 ? prefix + '/' + name : name;
            buf.Skip(12);
            IsDisposed = false;

            if (_length == 0 || TypeFlag != '0')
                return this;

            var msg = Statics.Expect<CopyDataMessage>(await _connector.ReadMessage(true), _connector);
            _currentMessageLength = msg.Length;
            return this;

            string ReadAsciiString(int byteLen)
            {
                var contentLen = 0;
                // Remove everything after the first null byte
                for (var i = buf.ReadPosition; i < buf.ReadPosition + byteLen; i++)
                {
                    if (buf.Buffer[i] == 0)
                        break;

                    contentLen++;
                }

                var result = Encoding.ASCII.GetString(buf.Buffer, buf.ReadPosition, contentLen);
                buf.ReadPosition += byteLen;
                return result;
            }

            int ReadOctalString(int byteLen)
            {
                Debug.Assert(byteLen > 0);
                var contentLen = byteLen;
                // Remove trailing padding
                for (var i = buf.ReadPosition + byteLen - 1; i >= buf.ReadPosition; i--)
                {
                    switch (buf.Buffer[i])
                    {
                    case 0:
                    case (byte)' ':
                        contentLen--;
                        continue;
                    }
                    break;
                }

                // There should be at least one padding byte
                Debug.Assert(contentLen < byteLen);
                Debug.Assert(contentLen > 0);

                var bin = 0;
                for (var i = buf.ReadPosition; i < buf.ReadPosition + contentLen; i++)
                {
                    bin *= 8;
                    bin += buf.Buffer[i] - '0';
                }
                buf.ReadPosition += byteLen;
                return bin;
            }

            // Like PostgreSQL's tarPaddingBytesRequired() this only works as long as TarBlockSize is a power of 2
            static int TarPaddingBytesRequired(int len)
                => checked((int)((((ulong)len + (ReplicationConnection.TarBlockSize - 1)) & ~(ulong)(ReplicationConnection.TarBlockSize - 1)) - (ulong)len));
        }

        /// <summary>
        /// The file name
        /// </summary>
        public string Name { get; private set; } = default!;

        /// <summary>
        /// The file mode
        /// </summary>
        public int Mode { get; private set; }

        /// <summary>
        /// The owner's numeric user ID
        /// </summary>
        public int UId { get; private set; }

        /// <summary>
        /// The owner's numeric group ID
        /// </summary>
        public int GId { get; private set; }

        /// <summary>
        /// The file size in bytes
        /// </summary>
        public long Size => _length;

        /// <summary>
        /// The file's last modification time
        /// </summary>
        public DateTime MTime { get; private set; }

        /// <summary>
        /// The checksum for the header record
        /// </summary>
        public int ChkSum { get; private set; }

        /// <summary>
        /// The file's type flag
        /// </summary>
        public char TypeFlag { get; private set; }

        /// <summary>
        /// The name of the linked file if this is a link
        /// </summary>
        public string LinkName { get; private set; } = default!;

        /// <summary>
        /// The owner's user name
        /// </summary>
        public string UName { get; private set; } = default!;

        /// <summary>
        /// The owner's group name
        /// </summary>
        public string GName { get; private set; } = default!;

        /// <summary>
        /// The device major number if this is a device
        /// </summary>
        public int DevMajor { get; private set; }

        /// <summary>
        /// The device major number if this is a device
        /// </summary>
        public int DevMinor { get; private set; }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
            => Read(new Span<byte>(buffer, offset, count));

#if NETSTANDARD2_0
        /// <summary>
        /// 
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public int Read(Span<byte> span)
#else
            /// <inheritdoc />
            public override int Read(Span<byte> span)
#endif
        {
            CheckDisposed();

            var count = Math.Min(span.Length, _length - _read);

            if (count == 0)
                return 0;


            if (_currentMessageLength - _currentMessageRead == 0)
            {
                var msg = Statics.Expect<CopyDataMessage>(_connector.ReadMessage(async: false).GetAwaiter().GetResult(), _connector);
                _currentMessageLength = msg.Length;
                _currentMessageRead = 0;
            }

            count = Math.Min(count, _currentMessageLength - _currentMessageRead);

            count = _connector.ReadBuffer.Read(span.Slice(0, count));
            _currentMessageRead += count;
            _read += count;

            return count;
        }

        /// <inheritdoc />
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();

#if NETSTANDARD2_0
        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
#else
        /// <inheritdoc />
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
#endif
        {
            CheckDisposed();

            var count = Math.Min(buffer.Length, _length - _read);

            if (count == 0)
                return new ValueTask<int>(0);

            using (NoSynchronizationContextScope.Enter())
            {
                var compoundToken = CancellationTokenSource.CreateLinkedTokenSource(_baseCancellationToken, cancellationToken).Token;
                return ReadAsyncInternal(this, buffer.Slice(0, count), compoundToken);
            }

            static async ValueTask<int> ReadAsyncInternal(TarFileStream stream, Memory<byte> buffer, CancellationToken cancellationToken)
            {
                var currentMessageBytesLeft = stream._currentMessageLength - stream._currentMessageRead;
                if (currentMessageBytesLeft == 0)
                {
                    using var _ = stream._connector.StartNestedCancellableOperation(cancellationToken, stream._connector.AttemptPostgresCancellation);
                    var msg = Statics.Expect<CopyDataMessage>(await stream._connector.ReadMessage(async: true), stream._connector);
                    stream._currentMessageLength = currentMessageBytesLeft = msg.Length;
                    stream._currentMessageRead = 0;
                }

                if (currentMessageBytesLeft < buffer.Length)
                {
                    buffer = buffer.Slice(0, currentMessageBytesLeft);
                }

                var count = await stream._connector.ReadBuffer.ReadAsync(buffer, cancellationToken);
                stream._currentMessageRead += count;
                stream._read += count;

                return count;
            }
        }

        internal bool IsDisposed { get; private set; }

        void CheckDisposed()
        {
            if (IsDisposed)
                throw new ObjectDisposedException(null);
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
            => DisposeAsync(disposing, async: false).GetAwaiter().GetResult();

#if NETSTANDARD2_0
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ValueTask DisposeAsync()
            => DisposeAsync(disposing: true, async: true);
#else
            /// <inheritdoc />
            public override ValueTask DisposeAsync()
                => DisposeAsync(disposing: true, async: true);
#endif

        internal async ValueTask DisposeAsync(bool disposing, bool async)
        {
            if (IsDisposed || !disposing)
                return;

            var leftToSkip = _currentMessageLength - _currentMessageRead;
            while (true)
            {
                if (leftToSkip > 0)
                {
                    var buf = _connector.ReadBuffer;
                    if (async)
                        await buf.Skip(leftToSkip, async: true);
                    else
                        buf.Skip(leftToSkip, async: false).GetAwaiter().GetResult();
                }

                _read += leftToSkip;
                if (_totalLength - _read > 0)
                {
                    var msg = Statics.Expect<CopyDataMessage>(async ? await _connector.ReadMessage(async: true) : _connector.ReadMessage(async: false).GetAwaiter().GetResult(), _connector);
                    leftToSkip = msg.Length;
                    continue;
                }

                break;
            }
            IsDisposed = true;
        }

        /// <summary>
        /// This method isn't supported and always throws a <see cref="NotSupportedException"/>.
        /// </summary>
        public override void Flush() => throw new NotSupportedException();
        /// <summary>
        /// This method isn't supported and always throws a <see cref="NotSupportedException"/>.
        /// </summary>
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        /// <summary>
        /// This method isn't supported and always throws a <see cref="NotSupportedException"/>.
        /// </summary>
        public override void SetLength(long value) => throw new NotSupportedException();
        /// <summary>
        /// This method isn't supported and always throws a <see cref="NotSupportedException"/>.
        /// </summary>
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        /// <inheritdoc />
        public override long Length => _length;

        /// <inheritdoc />
        /// <remarks>Setting isn't supported and always throws a <see cref="NotSupportedException"/>.</remarks>
        public override long Position
        {
            get => _read;
            set => throw new NotSupportedException();
        }

        /// <inheritdoc />
        /// <remarks>Always returns <see langword="true"/>.</remarks>
        public override bool CanRead => true;
        /// <inheritdoc />
        /// <remarks>Always returns <see langword="false"/>.</remarks>
        public override bool CanSeek => false;
        /// <inheritdoc />
        /// <remarks>Always returns <see langword="false"/>.</remarks>
        public override bool CanWrite => false;
    }
}
