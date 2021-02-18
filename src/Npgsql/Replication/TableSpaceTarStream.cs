using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.Util;

namespace Npgsql.Replication
{
    /// <summary>
    /// 
    /// </summary>
    public sealed class TableSpaceTarStream : Stream, ITableSpaceEnumerable
    {
        readonly PgTableSpaceInfo _info;
        readonly NpgsqlConnector _connector;
        readonly CancellationToken _baseCancellationToken;
        //bool _readingDone;

        internal TableSpaceTarStream(PgTableSpaceInfo info, NpgsqlConnector connector, CancellationToken baseCancellationToken)
        {
            _info = info;
            _connector = connector;
            _baseCancellationToken = baseCancellationToken;
        }

        #region ITableSpaceEnumerable

        /// <summary>
        /// 
        /// </summary>
        public uint? Oid => _info.Oid;

        /// <summary>
        /// 
        /// </summary>
        public string? Path => _info.Path;

        /// <summary>
        /// 
        /// </summary>
        public ulong? ApproximateSize => _info.Size;

        BackupResponseKind IBackupResponse.Kind => BackupResponseKind.TablespaceMessage;

        IAsyncEnumerator<PgTarFileStream> IAsyncEnumerable<PgTarFileStream>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            using (NoSynchronizationContextScope.Enter())
                return GetAsyncEnumeratorInternal(
                    CancellationTokenSource.CreateLinkedTokenSource(_baseCancellationToken, cancellationToken).Token);

            async IAsyncEnumerator<PgTarFileStream> GetAsyncEnumeratorInternal(CancellationToken innerCancellationToken)
            {
                while (true)
                {
                    var m = await _connector.ReadMessage(true);
                    if (m is not CopyDataMessage cdm)
                    {
                        Statics.Expect<CopyDoneMessage>(m, _connector);
                        //_readingDone = true;
                        yield break;
                    }

                    // We are at the beginning of a tar file, so this must be the header
                    if (cdm.Length != ReplicationConnection.TarBlockSize)
                        throw new NpgsqlException($"Invalid tar block header size: {cdm.Length}");

                    yield return await PgTarFileStream.Instance.Load(_connector, innerCancellationToken);
                    // Make sure we skip unconsumed bytes
                    await PgTarFileStream.Instance.DisposeAsync(true, true);
                }
            }
        }

        #endregion // ITableSpaceEnumerable

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

        /// <inheritdoc />
        public 
#if !NETSTANDARD2_0
            override
#endif
        ValueTask DisposeAsync()
        {
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
