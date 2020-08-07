using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication.Logical;
using Npgsql.Replication.Logical.Protocol;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    public class RawLogicalReplicationTests : SafeReplicationTestBase<NpgsqlLogicalReplicationConnection>
    {

    }
}
