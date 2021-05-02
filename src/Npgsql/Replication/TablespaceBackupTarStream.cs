using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using static Npgsql.Util.Statics;

namespace Npgsql.Replication
{
    /// <summary>
    /// 
    /// </summary>
    public sealed class TablespaceBackupTarStream : Stream, IBackupResponse, IAsyncEnumerable<TarFileStream>, IDisposable, IAsyncDisposable
    {
        internal TablespaceBackupTarStream(NpgsqlConnector connector, CancellationToken baseCancellationToken)
        {
            _connector = connector;
            _baseCancellationToken = baseCancellationToken;
        }

        readonly NpgsqlConnector _connector = default!;
        readonly CancellationToken _baseCancellationToken;
        bool _readingDone;

        BackupResponseKind IBackupResponse.Kind => BackupResponseKind.TablespaceDataMessage;

        IAsyncEnumerator<TarFileStream> IAsyncEnumerable<TarFileStream>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            using (NoSynchronizationContextScope.Enter())
                return GetAsyncEnumeratorInternal(_baseCancellationToken.CanBeCanceled || cancellationToken.CanBeCanceled
                    ? CancellationTokenSource.CreateLinkedTokenSource(_baseCancellationToken, cancellationToken).Token
                    : default);

            async IAsyncEnumerator<TarFileStream> GetAsyncEnumeratorInternal(CancellationToken innerCancellationToken)
            {
                while (true)
                {
                    var m = await _connector.ReadMessage(async: true);
                    if (m.Code != BackendMessageCode.CopyData)
                    {
                        Expect<CopyDoneMessage>(m, _connector);
                        _readingDone = true;
                        yield break;
                    }

                    // We are at the beginning of a tar file, so this must be the header
                    Debug.Assert(((CopyDataMessage)m).Length == ReplicationConnection.TarBlockSize, $"Invalid tar block header size: {((CopyDataMessage)m).Length}");

                    yield return await TarFileStream.Instance.Load(_connector, innerCancellationToken);
                    // Make sure we skip unconsumed bytes
                    await TarFileStream.Instance.DisposeAsync(true, true);
                }
            }
        }

        #region Stream

        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return base.ReadAsync(buffer, offset, count, cancellationToken);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public 
#if !NETSTANDARD2_0
        override
#endif
        int Read(Span<byte> span)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

#pragma warning disable CS1998 // Bei der asynchronen Methode fehlen "await"-Operatoren. Die Methode wird synchron ausgeführt.
        /// <inheritdoc />
        public async
#if !NETSTANDARD2_0
            override
#endif
        ValueTask DisposeAsync()
#pragma warning restore CS1998 // Bei der asynchronen Methode fehlen "await"-Operatoren. Die Methode wird synchron ausgeführt.
        {
            if (_readingDone)
            {
                return;
            }
            throw new NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Flush() => throw new NotSupportedException();
        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="origin"></param>
        /// <returns></returns>
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public override void SetLength(long value) => throw new NotSupportedException();
        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        /// <summary>
        /// 
        /// </summary>
        public override bool CanRead => true;
        /// <summary>
        /// 
        /// </summary>
        public override bool CanSeek => false;
        /// <summary>
        /// 
        /// </summary>
        public override bool CanWrite => false;
        /// <summary>
        /// 
        /// </summary>
        public override long Length  => throw new NotSupportedException();
        /// <summary>
        /// 
        /// </summary>
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        #endregion // Stream
    }
}
