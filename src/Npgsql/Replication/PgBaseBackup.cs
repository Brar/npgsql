//using System;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.Threading;
//using System.Threading.Tasks;
//using Npgsql.BackendMessages;
//using NpgsqlTypes;
//using static Npgsql.Util.Statics;

//namespace Npgsql.Replication
//{
//    /// <summary>
//    /// 
//    /// </summary>
//    public class PgBaseBackup : IAsyncEnumerable<TableSpaceTarStream>
//    {
//        internal static async Task<PgBaseBackup> CreateInstance(NpgsqlConnector connector, CancellationToken baseCancellationToken)
//        {

//            // The second result set contains one row for each tablespace with the following fields:
//            // - The OID of the tablespace, or null if it's the base directory.
//            // - The full path of the tablespace directory, or null if it's the base directory.
//            // - The approximate size of the tablespace, in kilobytes (1024 bytes), if progress report has been requested; otherwise it's null.
//            description =
//                Expect<RowDescriptionMessage>(await connector.ReadMessage(true), connector);
//            Debug.Assert(description.Fields.Count == 3);
//            Debug.Assert(description.Fields[0].PostgresType.Name == "oid");
//            Debug.Assert(description.Fields[1].PostgresType.Name == "text");
//            Debug.Assert(description.Fields[2].PostgresType.Name == "bigint");

//            IBackendMessage msg = Expect<DataRowMessage>(await connector.ReadMessage(true), connector);
//            var infos = new PgTableSpaceInfo[1];
//            var pos = 0;
//            while (true)
//            {
//                await buf.EnsureAsync(2);
//                cols = buf.ReadInt16();
//                Debug.Assert(cols == 3);

//                uint? oidspcoid = null;
//                await buf.EnsureAsync(4);
//                len = buf.ReadInt32();
//                if (len > -1)
//                {
//                    await buf.EnsureAsync(len);
//                    oidspcoid = uint.Parse(buf.ReadString(len));
//                }

//                string? spclocation = null;
//                await buf.EnsureAsync(4);
//                len = buf.ReadInt32();
//                if (len > -1)
//                {
//                    await buf.EnsureAsync(len);
//                    spclocation = buf.ReadString(len);
//                }

//                ulong? size = null;
//                await buf.EnsureAsync(4);
//                len = buf.ReadInt32();
//                if (len > -1)
//                {
//                    await buf.EnsureAsync(len);
//                    size = ulong.Parse(buf.ReadString(len));
//                }

//                infos[pos++] = new PgTableSpaceInfo(oidspcoid, spclocation, size);
//                msg = await connector.ReadMessage(true);
//                if (msg.Code != BackendMessageCode.DataRow)
//                    break;

//                var newInfos = new PgTableSpaceInfo[pos + 1];
//                Array.Copy(infos, newInfos, pos);
//                infos = newInfos;
//            }

//            Expect<CommandCompleteMessage>(msg, connector);
//            return new PgBaseBackup(startPosition, timelineId, infos, connector, baseCancellationToken);
//        }

//        readonly PgTableSpaceInfo[] _tableSpaceInfos;
//        readonly NpgsqlConnector _connector;
//        readonly CancellationToken _baseCancellationToken;

//        PgBaseBackup(NpgsqlLogSequenceNumber startPosition, uint timelineId, PgTableSpaceInfo[] tableSpaceInfos, NpgsqlConnector connector, CancellationToken baseCancellationToken)
//        {
//            StartPosition = startPosition;
//            TimelineId = timelineId;
//            _tableSpaceInfos = tableSpaceInfos;
//            _connector = connector;
//            _baseCancellationToken = baseCancellationToken;
//        }

//        /// <summary>
//        /// 
//        /// </summary>
//        public NpgsqlLogSequenceNumber StartPosition { get; }

//        /// <summary>
//        /// 
//        /// </summary>
//        public uint TimelineId { get; }

//        /// <summary>
//        /// 
//        /// </summary>
//        /// <param name="cancellationToken"></param>
//        /// <returns></returns>
//        /// <exception cref="NotImplementedException"></exception>
//        public IAsyncEnumerator<TableSpaceTarStream> GetAsyncEnumerator(CancellationToken cancellationToken = default)
//        {
//            using (NoSynchronizationContextScope.Enter())
//                return GetAsyncEnumeratorInternal(
//                    CancellationTokenSource.CreateLinkedTokenSource(_baseCancellationToken, cancellationToken).Token);


//            async IAsyncEnumerator<TableSpaceTarStream> GetAsyncEnumeratorInternal(CancellationToken innerCancellationToken)
//            {
//                foreach (var info in _tableSpaceInfos)
//                {
//                    Expect<CopyOutResponseMessage>(await _connector.ReadMessage(async: true), _connector);
//                    yield return new TableSpaceTarStream(info, _connector, innerCancellationToken);
//                }

//                var msg = await _connector.ReadMessage(true);
//                var buf = _connector.ReadBuffer;

//                RowDescriptionMessage description;
//                if (msg.Code != BackendMessageCode.CopyOutResponse)
//                    description = Expect<RowDescriptionMessage>(msg, _connector);
//                else // If a backup manifest was requested, another CopyResponse result is sent
//                {
//                    while (true)
//                    {
//                        msg = await _connector.ReadMessage(async: true);
//                        if (msg is CopyDataMessage cdm)
//                        {
//                            await buf.Ensure(cdm.Length, async: true);
//                            var test = buf.ReadString(cdm.Length);
//                            Debug.Write(test);
//                            // Todo: implement
//                        }
//                        else
//                        {
//                            Debug.WriteLine(string.Empty);
//                            Expect<CopyDoneMessage>(msg, _connector);
//                            break;
//                        }
//                    }

//                    description = Expect<RowDescriptionMessage>(await _connector.ReadMessage(true), _connector);
//                }

//                Debug.Assert(description.Fields.Count == 2);
//                Debug.Assert(description.Fields[0].PostgresType.Name == "text");
//                Debug.Assert(description.Fields[1].PostgresType.Name == "bigint");
//                Expect<DataRowMessage>(await _connector.ReadMessage(true), _connector);
//                await buf.EnsureAsync(2);
//                var cols = buf.ReadInt16();
//                Debug.Assert(cols == 2);

//                await buf.EnsureAsync(4);
//                var len = buf.ReadInt32();
//                Debug.Assert(len > 0);
//                await buf.EnsureAsync(len);
//                var endPosition = NpgsqlLogSequenceNumber.Parse(buf.ReadString(len));
//                Debug.WriteLine(endPosition);

//                await buf.EnsureAsync(4);
//                len = buf.ReadInt32();
//                Debug.Assert(len > 0);
//                await buf.EnsureAsync(len);
//                var timelineId = uint.Parse(buf.ReadString(len));
//                Debug.WriteLine(timelineId);
//                Expect<CommandCompleteMessage>(await _connector.ReadMessage(true), _connector);
//                Expect<CommandCompleteMessage>(await _connector.ReadMessage(true), _connector);

//                Expect<ReadyForQueryMessage>(await _connector.ReadMessage(true), _connector);
//            }
//        }
//    }
//}
