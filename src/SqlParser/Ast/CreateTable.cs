﻿namespace SqlParser.Ast;

public record CreateTable([property: Visit(0)] ObjectName Name, [property: Visit(1)] Sequence<ColumnDef> Columns) : IWriteSql, IIfNotExists
{
    public bool OrReplace { get; init; }
    public bool Temporary { get; init; }
    public bool External { get; init; }
    public bool? Global { get; init; }
    public bool IfNotExists { get; init; }
    public bool Transient { get; init; }
    [Visit(2)] public Sequence<TableConstraint>? Constraints { get; init; }
    public HiveDistributionStyle? HiveDistribution { get; init; } = new HiveDistributionStyle.None();
    public HiveFormat? HiveFormats { get; init; }
    public Sequence<SqlOption>? TableProperties { get; init; }
    public Sequence<SqlOption>? WithOptions { get; init; }
    public FileFormat FileFormat { get; init; }
    public string? Location { get; init; }
    [Visit(3)] public Query? Query { get; init; }
    public bool WithoutRowId { get; init; }
    [Visit(4)] public ObjectName? Like { get; init; }
    [Visit(5)] public ObjectName? CloneClause { get; init; }
    public string? Engine { get; init; }
    // ReSharper disable once MemberHidesStaticFromOuterClass
    public new string? Comment { get; init; }
    public Sequence<Ident>? OrderBy { get; init; }
    public Expression? PartitionBy { get; init; }
    public Sequence<Ident>? ClusterBy { get; init; }
    public Sequence<SqlOption>? Options { get; init; }
    public int? AutoIncrementOffset { get; init; }
    public string? DefaultCharset { get; init; }
    public string? Collation { get; init; }
    public OnCommit OnCommit { get; init; }
    // Clickhouse "ON CLUSTER" clause:
    // https://clickhouse.com/docs/en/sql-reference/distributed-ddl/
    public string? OnCluster { get; init; }
    // SQLite "STRICT" clause.
    // if the "STRICT" table-option keyword is added to the end, after the closing ")",
    // then strict typing rules apply to that table.
    public bool Strict { get; init; }
    
    public void ToSql(SqlTextWriter writer)
    {
        var orReplace = OrReplace ? "OR REPLACE " : null;
        var external = External ? "EXTERNAL " : null;
        var global = Global.HasValue ? Global.Value ? "GLOBAL " : "LOCAL " : null;
        var temp = Temporary ? "TEMPORARY " : null;
        var transient = Transient ? "TRANSIENT " : null;
        var ifNot = IfNotExists ? $"{((IIfNotExists)this).IfNotExistsText} " : null;
        writer.WriteSql($"CREATE {orReplace}{external}{global}{temp}{transient}TABLE {ifNot}{Name}");

        if (OnCluster != null)
        {
            var cluster = OnCluster
                .Replace(Symbols.CurlyBracketOpen.ToString(), $"{Symbols.SingleQuote}{Symbols.CurlyBracketOpen}")
                .Replace(Symbols.CurlyBracketClose.ToString(), $"{Symbols.CurlyBracketClose}{Symbols.SingleQuote}");
            writer.WriteSql($" ON CLUSTER {cluster}");
        }

        var hasColumns = Columns.SafeAny();
        var hasConstraints = Constraints.SafeAny();

        if (hasColumns || hasConstraints)
        {
            writer.WriteSql($" ({Columns}");

            if (hasColumns && hasConstraints)
            {
                writer.Write(", ");
            }

            writer.WriteSql($"{Constraints})");
        }
        else if (Query == null && Like == null && CloneClause == null)
        {
            // PostgreSQL allows `CREATE TABLE t ();`, but requires empty parens
            writer.Write(" ()");
        }

        // Only for SQLite
        if (WithoutRowId)
        {
            writer.Write(" WITHOUT ROWID");
        }

        // Only for Hive
        if (Like != null)
        {
            writer.WriteSql($" LIKE {Like}");
        }

        if (CloneClause != null)
        {
            writer.WriteSql($" CLONE {CloneClause}");
        }

        if (HiveDistribution is HiveDistributionStyle.Partitioned part)
        {
            writer.WriteSql($" PARTITIONED BY ({part.Columns.ToSqlDelimited()})");
        }
        else if (HiveDistribution is HiveDistributionStyle.Clustered clustered)
        {
            writer.WriteSql($" CLUSTERED BY ({clustered.Columns.ToSqlDelimited()})");

            if (clustered.SortedBy.SafeAny())
            {
                writer.WriteSql($" SORTED BY ({clustered.SortedBy.ToSqlDelimited()})");
            }

            if (clustered.NumBuckets > 0)
            {
                writer.WriteSql($" INTO {clustered.NumBuckets} BUCKETS");
            }
        }
        else if (HiveDistribution is HiveDistributionStyle.Skewed skewed)
        {
            writer.WriteSql($" SKEWED BY ({skewed.Columns.ToSqlDelimited()}) ON ({skewed.On.ToSqlDelimited()})");
        }

        if (HiveFormats != null)
        {
            switch (HiveFormats.RowFormat)
            {
                case HiveRowFormat.Serde serde:
                    writer.WriteSql($" ROW FORMAT SERDE '{serde.Class}'");
                    break;
                case HiveRowFormat.Delimited d:
                    writer.WriteSql($" ROW FORMAT DELIMITED");
                    if (d.Delimiters.SafeAny())
                    {
                        writer.Write($" {d.Delimiters.ToSqlDelimited(Symbols.Space)}");
                    }
                    break;
            }

            if (HiveFormats.Storage != null)
            {
                switch (HiveFormats.Storage)
                {
                    case HiveIOFormat.IOF iof:
                        // ReSharper disable once StringLiteralTypo
                        writer.WriteSql($" STORED AS INPUTFORMAT {iof.InputFormat.ToSql()} OUTPUTFORMAT {iof.OutputFormat.ToSql()}");
                        break;

                    case HiveIOFormat.FileFormat ff when !External:
                        writer.WriteSql($" STORED AS {ff.Format}");
                        break;
                }

                if (HiveFormats.SerdeProperties.SafeAny())
                {
                    writer.Write($" WITH SERDEPROPERTIES ({HiveFormats.SerdeProperties.ToSqlDelimited()})");
                }

                if (!External)
                {
                    writer.WriteSql($" LOCATION '{HiveFormats.Location}'");
                }
            }
        }

        if (External)
        {
            writer.WriteSql($" STORED AS {FileFormat} LOCATION '{Location}'");
        }

        if (TableProperties.SafeAny())
        {
            writer.WriteSql($" TBLPROPERTIES ({TableProperties})");
        }

        if (WithOptions.SafeAny())
        {
            writer.WriteSql($" WITH ({WithOptions})");
        }

        if (Engine != null)
        {
            writer.WriteSql($" ENGINE={Engine}");
        }

        if (Comment != null)
        {
            writer.WriteSql($" COMMENT '{Comment}'");
        }

        if (AutoIncrementOffset != null)
        {
            writer.Write($" AUTO_INCREMENT {AutoIncrementOffset.Value}");
        }

        if (OrderBy.SafeAny())
        {
            writer.WriteSql($" ORDER BY ({OrderBy})");
        }

        if (PartitionBy != null)
        {
            writer.WriteSql($" PARTITION BY {PartitionBy}");
        }

        if (ClusterBy != null)
        {
            writer.WriteSql($" CLUSTER BY {ClusterBy}");
        }

        if (Options.SafeAny())
        {
            writer.Write($"OPTIONS({Options.ToSqlDelimited()})");
        }

        if (Query != null)
        {
            writer.WriteSql($" AS {Query}");
        }

        if (DefaultCharset != null)
        {
            writer.WriteSql($" DEFAULT CHARSET={DefaultCharset}");
        }

        if (Collation != null)
        {
            writer.WriteSql($" COLLATE={Collation}");
        }

        switch (OnCommit)
        {
            case OnCommit.DeleteRows:
                writer.Write(" ON COMMIT DELETE ROWS");
                break;

            case OnCommit.PreserveRows:
                writer.Write(" ON COMMIT PRESERVE ROWS");
                break;

            case OnCommit.Drop:
                writer.Write(" ON COMMIT DROP");
                break;
        }

        if (Strict)
        {
            writer.Write(" STRICT");
        }
    }
}