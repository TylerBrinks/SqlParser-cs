namespace SqlParser.Ast;

public record CreateTable([property: Visit(0)] ObjectName Name, [property: Visit(1)] Sequence<ColumnDef> Columns) : IWriteSql, IIfNotExists
{
    public bool OrReplace { get; init; }
    public bool Temporary { get; init; }
    public bool External { get; init; }
    public bool? Global { get; init; }
    public bool IfNotExists { get; init; }
    public bool Transient { get; init; }
    public bool Volatile { get; init; }
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
    public TableEngine? Engine { get; init; }
    // ReSharper disable once MemberHidesStaticFromOuterClass
    public CommentDef? Comment { get; init; }
    public OneOrManyWithParens<Expression>? OrderBy { get; init; }
    public Expression? PartitionBy { get; init; }
    public WrappedCollection<Ident>? ClusterBy { get; init; }
    public ClusteredBy? ClusteredBy { get; init; }
    public Sequence<SqlOption>? Options { get; init; }
    public int? AutoIncrementOffset { get; init; }
    public string? DefaultCharset { get; init; }
    public string? Collation { get; init; }
    public OnCommit OnCommit { get; init; }
    // Clickhouse "ON CLUSTER" clause:
    // https://clickhouse.com/docs/en/sql-reference/distributed-ddl/
    public Ident? OnCluster { get; init; }
    public Expression? PrimaryKey { get; init; }
    // SQLite "STRICT" clause.
    // if the "STRICT" table-option keyword is added to the end, after the closing ")",
    // then strict typing rules apply to that table.
    public bool Strict { get; init; }
    public bool? CopyGrants { get; init; }
    public bool? EnableSchemaEvolution { get; init; }
    public bool? ChangeTracking { get; init; }
    public long? DataRetentionTimeInDays { get; init; }
    public long? MaxDataExtensionTimeInDays { get; init; }
    public string? DefaultDdlCollation { get; init; }
    public ObjectName? WithAggregationPolicy { get; init; }
    public RowAccessPolicy? WithRowAccessPolicy { get; init; }
    public Sequence<Tag>? WithTags { get; init; }
    
    public void ToSql(SqlTextWriter writer)
    {
        var orReplace = OrReplace ? "OR REPLACE " : null;
        var external = External ? "EXTERNAL " : null;
        var global = Global.HasValue ? Global.Value ? "GLOBAL " : "LOCAL " : null;
        var temp = Temporary ? "TEMPORARY " : null;
        var transient = Transient ? "TRANSIENT " : null;
        var ifNot = IfNotExists ? $"{((IIfNotExists)this).IfNotExistsText} " : null;
        var isVolatile = Volatile ? "VOLATILE " : null;

        writer.WriteSql($"CREATE {orReplace}{external}{global}{temp}{transient}{isVolatile}TABLE {ifNot}{Name}");

        if (OnCluster != null)
        {
            writer.WriteSql($" ON CLUSTER {OnCluster}");
        }

        var hasColumns = Columns.SafeAny();
        var hasConstraints = Constraints.SafeAny();

        if (hasColumns || hasConstraints)
        {
            writer.WriteSql($" ({Columns}");

            if (hasColumns && hasConstraints)
            {
                writer.Write(Constants.SpacedComma);
            }

            writer.WriteSql($"{Constraints})");
        }
        else if (Query == null && Like == null && CloneClause == null)
        {
            // PostgreSQL allows `CREATE TABLE t ();`, but requires empty parens
            writer.Write(" ()");
        }

        // Hive table comment should be after column definitions, please refer to:
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable
        if(Comment is CommentDef.AfterColumnDefsWithoutEq a)
        {
            writer.WriteSql($" COMMENT '{a.Comment}'");
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

        switch (HiveDistribution)
        {
            case HiveDistributionStyle.Partitioned part:
                writer.WriteSql($" PARTITIONED BY ({part.Columns.ToSqlDelimited()})");
                break;

            case HiveDistributionStyle.Skewed skewed:
                writer.WriteSql($" SKEWED BY ({skewed.Columns.ToSqlDelimited()}) ON ({skewed.On.ToSqlDelimited()})");
                break;
        }

        if (ClusteredBy != null)
        {
            writer.WriteSql($" {ClusteredBy}");
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
            switch (Comment)
            {
                case CommentDef.WithEq we:
                    writer.WriteSql($" COMMENT = '{we.Comment}'");
                    break;
                case CommentDef.WithoutEq w:
                    writer.Write($" COMMENT '{w.Comment}'");
                    break;
            }
        }

        if (AutoIncrementOffset != null)
        {
            writer.Write($" AUTO_INCREMENT {AutoIncrementOffset.Value}");
        }

        if (PrimaryKey != null)
        {
            writer.WriteSql($" PRIMARY KEY {PrimaryKey}");
        }

        if (OrderBy != null)
        {
            writer.WriteSql($" ORDER BY {OrderBy}");
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
            writer.Write($" OPTIONS({Options.ToSqlDelimited()})");
        }

        if (CopyGrants.HasValue)
        {
            writer.Write(" COPY GRANTS");
        }

        if (EnableSchemaEvolution.HasValue)
        {
            var evolution = EnableSchemaEvolution.Value ? "TRUE" : "FALSE";
            writer.Write($" ENABLE_SCHEMA_EVOLUTION={evolution}");
        }

        if (ChangeTracking.HasValue)
        {
            var tracking = ChangeTracking.Value ? "TRUE" : "FALSE";
            writer.Write($" CHANGE_TRACKING={tracking}");
        }

        if (DataRetentionTimeInDays.HasValue)
        {
            writer.Write($" DATA_RETENTION_TIME_IN_DAYS={DataRetentionTimeInDays.Value}");
        }

        if (MaxDataExtensionTimeInDays.HasValue)
        {
            writer.Write($" MAX_DATA_EXTENSION_TIME_IN_DAYS={MaxDataExtensionTimeInDays.Value}");
        }

        if (DefaultDdlCollation != null)
        {
            writer.Write($" DEFAULT_DDL_COLLATION='{DefaultDdlCollation}'");
        }

        if (WithAggregationPolicy != null)
        {
            writer.Write($" WITH AGGREGATION POLICY {WithAggregationPolicy}");
        }

        if (WithTags.SafeAny())
        {
            writer.WriteSql($" WITH TAG ({WithTags.ToSqlDelimited()})");
        }

        if (WithRowAccessPolicy != null)
        {
            writer.WriteSql($" {WithRowAccessPolicy}");
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

        if (Query != null)
        {
            writer.WriteSql($" AS {Query}");
        }
    }
}

public record ClusteredBy(Sequence<Ident> Columns, Sequence<OrderByExpression>? SortedBy, Value NumBuckets) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"CLUSTERED BY ({Columns.ToSqlDelimited()})");

        if (SortedBy.SafeAny())
        {
            writer.WriteSql($" SORTED BY ({SortedBy.ToSqlDelimited()})");
        }

        writer.WriteSql($" INTO {NumBuckets} BUCKETS");
    }
}