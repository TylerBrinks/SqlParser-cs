namespace SqlParser.Ast;

/// <summary>
/// VACUUM statement - PostgreSQL/Redshift
/// </summary>
public record VacuumStatement : IWriteSql, IElement
{
    public ObjectName? TableName { get; init; }
    public Sequence<Ident>? Columns { get; init; }
    public Sequence<VacuumOption>? Options { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("VACUUM");

        if (Options.SafeAny())
        {
            writer.WriteSql($" ({Options.ToSqlDelimited()})");
        }

        if (TableName != null)
        {
            writer.WriteSql($" {TableName}");

            if (Columns.SafeAny())
            {
                writer.WriteSql($" ({Columns.ToSqlDelimited()})");
            }
        }
    }
}

/// <summary>
/// VACUUM options
/// </summary>
public abstract record VacuumOption : IWriteSql, IElement
{
    public record Full : VacuumOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FULL");
        }
    }

    public record Freeze : VacuumOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FREEZE");
        }
    }

    public record Verbose : VacuumOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("VERBOSE");
        }
    }

    public record Analyze : VacuumOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("ANALYZE");
        }
    }

    public record DisablePageSkipping : VacuumOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DISABLE_PAGE_SKIPPING");
        }
    }

    public record SkipLocked : VacuumOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SKIP_LOCKED");
        }
    }

    public record IndexCleanup : VacuumOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("INDEX_CLEANUP");
        }
    }

    public record ProcessToast : VacuumOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("PROCESS_TOAST");
        }
    }

    public record Truncate : VacuumOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TRUNCATE");
        }
    }

    public record ParallelWorkers(int Workers) : VacuumOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"PARALLEL {Workers}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
