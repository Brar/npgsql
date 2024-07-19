using System;
using System.Text;

namespace Npgsql.Replication;

abstract class CreateReplicationSlotOptions
{
    static readonly Version FirstVersionWithTemporarySlots = new(10, 0);
    protected static readonly Version FirstVersionWithNewOptionsSyntax = new(15, 0);

    public required string SlotName { get; init; }
    public bool Temporary { get; init; }
    public abstract string ValidateAndCreateCommand(Version postgreSqlVersion);

    public void ValidateCommon(Version postgreSqlVersion)
    {
        if (Temporary && postgreSqlVersion < FirstVersionWithTemporarySlots)
        {
            throw new NotSupportedException("Temporary replication slots were introduced in PostgreSQL " +
                                            $"{FirstVersionWithTemporarySlots.ToString(1)}. " +
                                            $"Using PostgreSQL version {postgreSqlVersion.ToString(3)} you " +
                                            "have to set the isTemporary argument to false.");
        }
    }
}

sealed class CreatePhysicalReplicationSlotOptions : CreateReplicationSlotOptions
{
    public bool ReserveWal { get; init; }
    public override string ValidateAndCreateCommand(Version postgreSqlVersion)
    {
        ValidateCommon(postgreSqlVersion);

        var builder = new StringBuilder("CREATE_REPLICATION_SLOT ")
            .Append(SlotName);
        if (Temporary)
            builder.Append(" TEMPORARY");
        builder.Append(" PHYSICAL");
        if (ReserveWal)
            builder.Append(postgreSqlVersion >= FirstVersionWithNewOptionsSyntax ? " (RESERVE_WAL)" : " RESERVE_WAL");

        return builder.ToString();
    }
}

sealed class CreateLogicalReplicationSlotOptions : CreateReplicationSlotOptions
{
    static readonly Version FirstVersionWithTwoPhaseSupport = new(15, 0);
    static readonly Version FirstVersionWithSlotSnapshotInitMode = new(10, 0);

    public required string OutputPlugin { get; init; }
    public LogicalSlotSnapshotInitMode? SnapshotInitMode { get; init; }
    public bool TwoPhase { get; init; }
    public override string ValidateAndCreateCommand(Version postgreSqlVersion)
    {
        ValidateCommon(postgreSqlVersion);
        if (TwoPhase && postgreSqlVersion < FirstVersionWithTwoPhaseSupport)
        {
            throw new NotSupportedException("Logical replication support for prepared transactions was introduced in PostgreSQL " +
                                            FirstVersionWithTwoPhaseSupport.ToString(1) +
                                            ". Using PostgreSQL version " +
                                            (postgreSqlVersion.Build == -1
                                                ? postgreSqlVersion.ToString(2)
                                                : postgreSqlVersion.ToString(3)) +
                                            " you have to set the twoPhase argument to false.");
        }

        if (SnapshotInitMode.HasValue && postgreSqlVersion < FirstVersionWithSlotSnapshotInitMode)
        {
            throw new NotSupportedException(
                "The EXPORT_SNAPSHOT, USE_SNAPSHOT and NOEXPORT_SNAPSHOT syntax was introduced in PostgreSQL " +
                $"{FirstVersionWithSlotSnapshotInitMode.ToString(1)}. Using PostgreSQL version " +
                $"{postgreSqlVersion.ToString(3)} you have to omit the slotSnapshotInitMode argument.");
        }

        var builder = new StringBuilder("CREATE_REPLICATION_SLOT ")
            .Append(SlotName);
        if (Temporary)
            builder.Append(" TEMPORARY");
        builder.Append(" LOGICAL ")
            .Append(OutputPlugin);
        if (postgreSqlVersion >= FirstVersionWithNewOptionsSyntax && (SnapshotInitMode.HasValue || TwoPhase))
        {
            builder.Append('(');
            if (SnapshotInitMode.HasValue)
            {
                builder.Append(SnapshotInitMode switch
                {
                    LogicalSlotSnapshotInitMode.Export => "SNAPSHOT 'export'",
                    LogicalSlotSnapshotInitMode.Use => "SNAPSHOT 'use'",
                    LogicalSlotSnapshotInitMode.NoExport => "SNAPSHOT 'nothing'",
                    _ => throw new ArgumentOutOfRangeException(nameof(SnapshotInitMode),
                        SnapshotInitMode,
                        $"Unexpected value {SnapshotInitMode} for argument {nameof(SnapshotInitMode)}.")
                });
                if (TwoPhase)
                    builder.Append(",TWO_PHASE");
            }
            else
                builder.Append("TWO_PHASE");
            builder.Append(')');
        }
        else
        {
            builder.Append(SnapshotInitMode switch
            {
                // EXPORT_SNAPSHOT is the default since it has been introduced.
                // We don't set it unless it is explicitly requested so that older backends can digest the query too.
                null => string.Empty,
                LogicalSlotSnapshotInitMode.Export => " EXPORT_SNAPSHOT",
                LogicalSlotSnapshotInitMode.Use => " USE_SNAPSHOT",
                LogicalSlotSnapshotInitMode.NoExport => " NOEXPORT_SNAPSHOT",
                _ => throw new ArgumentOutOfRangeException(nameof(SnapshotInitMode),
                    SnapshotInitMode,
                    $"Unexpected value {SnapshotInitMode} for argument {nameof(SnapshotInitMode)}.")
            });
            if (TwoPhase)
                builder.Append(" TWO_PHASE");
        }
        return builder.ToString();
    }
}
