// ReSharper disable StringLiteralTypo
// ReSharper disable CommentTypo
namespace SqlParser.Ast;

public abstract record Statement : IWriteSql, IElement
{
    /// <summary>
    /// Alter index statement
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="Operation">Index operation</param>
    public record AlterIndex(ObjectName Name, AlterIndexOperation Operation) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ALTER INDEX {Name} {Operation}");
        }
    }
    /// <summary>
    /// Alter table statement
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="Operation">Table operation</param>
    public record AlterTable(ObjectName Name, AlterTableOperation Operation) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ALTER TABLE {Name} {Operation}");
        }
    }
    /// <summary>
    /// Analyze statement
    /// </summary>
    public record Analyze([property: Visit(0)] ObjectName Name) : Statement
    {
        [Visit(1)] public Sequence<Expression>? Partitions { get; init; }
        public bool ForColumns { get; init; }
        public Sequence<Ident>? Columns { get; init; }
        public bool CacheMetadata { get; init; }
        public bool NoScan { get; init; }
        public bool ComputeStatistics { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ANALYZE TABLE {Name}");

            if (Partitions.SafeAny())
            {
                writer.WriteSql($" PARTITION ({Partitions})");
            }

            if (ComputeStatistics)
            {
                writer.Write(" COMPUTE STATISTICS");
            }

            if (NoScan)
            {
                writer.Write(" NOSCAN");
            }

            if (CacheMetadata)
            {
                writer.Write(" CACHE METADATA");
            }

            if (ForColumns)
            {
                writer.Write(" FOR COLUMNS");
                if (Columns.SafeAny())
                {
                    writer.WriteSql($" ({Columns})");
                    //writer.WriteCommaDelimited(Columns);
                }
            }
        }
    }
    /// <summary>
    /// Assert statement
    /// </summary>
    /// <param name="Condition">Condition</param>
    /// <param name="Message">Message</param>
    public record Assert(Expression Condition, Expression? Message = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ASSERT {Condition}");
            if (Message != null)
            {
                writer.WriteSql($" AS {Message}");
            }
        }
    }
    /// <summary>
    /// Assignment statement
    /// </summary>
    /// <param name="Id">ID List</param>
    /// <param name="Value">Expression value</param>
    public record Assignment(Sequence<Ident> Id, Expression Value) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteDelimited(Id, ".");
            writer.WriteSql($" = {Value}");
        }
    }
    /// <summary>
    /// Cache statement
    /// See Spark SQL docs for more details.
    /// <see href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-aux-cache-cache-table.html"/>
    ///
    /// <example>
    /// <c>
    /// CACHE [ FLAG ] TABLE table_name [ OPTIONS('K1' = 'V1', 'K2' = V2) ] [ AS ] [ query 
    /// </c>
    /// </example>
    /// </summary>
    public record Cache([property: Visit(1)] ObjectName Name) : Statement
    {
        /// <summary>
        /// Table flag
        /// </summary>
        [Visit(0)] public ObjectName? TableFlag { get; init; }
        public bool HasAs { get; init; }
        [Visit(2)] public Sequence<SqlOption>? Options { get; init; }
        /// <summary>
        /// Cache table as a Select
        /// </summary>
        [Visit(3)] public Select? Query { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (TableFlag != null)
            {
                writer.WriteSql($"CACHE {TableFlag} TABLE {Name}");
            }
            else
            {
                writer.WriteSql($"CACHE TABLE {Name}");
            }

            if (Options != null)
            {
                writer.WriteSql($" OPTIONS({Options})");
            }

            switch (HasAs)
            {
                case true when Query != null:
                    writer.WriteSql($" AS {Query}");
                    break;

                case false when Query != null:
                    writer.WriteSql($" {Query}");
                    break;

                case true when Query == null:
                    writer.Write(" AS");
                    break;
            }
        }
    }
    /// <summary>
    /// Closes statement closes the portal underlying an open cursor.
    /// </summary>
    /// <param name="Cursor">Cursor to close</param>
    public record Close(CloseCursor Cursor) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"CLOSE {Cursor}");
        }
    }
    /// <summary>
    /// 
    /// </summary>
    /// <param name="Name">Name</param>
    /// <param name="ObjectType">Comment object type</param>
    /// <param name="Value">Comment value</param>
    /// <param name="IfExists">Optional IF EXISTS clause</param>
    public record Comment(ObjectName Name, CommentObject ObjectType, string? Value = null, bool IfExists = false) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("COMMENT ");
            if (IfExists)
            {
                writer.Write("IF EXISTS ");
            }

            writer.WriteSql($"ON {ObjectType}");
            writer.WriteSql($" {Name} IS ");
            writer.Write(Value != null ? $"'{Value}'" : "NULL");
        }
    }
    /// <summary>
    /// Commit statement
    /// </summary>
    /// <param name="Chain">True if chained</param>
    public record Commit(bool Chain = false) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var chain = Chain ? " AND CHAIN" : null;
            writer.WriteSql($"COMMIT{chain}");
        }
    }
    /// <summary>
    /// Copy statement
    /// 
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="Columns">Columns</param>
    /// <param name="To">True if to</param>
    /// <param name="Target">Copy target</param>
    public record Copy(ObjectName Name, Sequence<Ident>? Columns, bool To, CopyTarget Target) : Statement
    {
        public Sequence<CopyOption>? Options { get; init; }
        // WITH options (before PostgreSQL version 9.0)
        public Sequence<CopyLegacyOption>? LegacyOptions { get; init; }
        // VALUES a vector of values to be copied
        public Sequence<string?>? Values { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"COPY {Name}");
            if (Columns.SafeAny())
            {
                writer.WriteSql($" ({Columns})");
            }

            var direction = To ? "TO" : "FROM";
            writer.WriteSql($" {direction} {Target}");

            if (Options.SafeAny())
            {
                writer.WriteSql($" ({Options})");
            }

            if (LegacyOptions.SafeAny())
            {
                writer.Write(" ");
                writer.WriteDelimited(LegacyOptions, " ");
            }

            if (Values.SafeAny())
            {
                writer.WriteLine(";");
                var delimiter = "";
                foreach (var value in Values!)
                {
                    writer.Write(delimiter);
                    delimiter = Symbols.Tab.ToString();

                    writer.Write(value ?? "\\N");
                }
                writer.WriteLine("\n\\.");
            }
        }
    }
    /// <summary>
    /// Create Database statement
    /// </summary>
    public record CreateDatabase(ObjectName Name) : Statement, IIfNotExists
    {
        public bool IfNotExists { get; init; }
        public string? Location { get; init; }
        public string? ManagedLocation { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("CREATE DATABASE");
            if (IfNotExists)
            {
                writer.Write($" {AsIne.IfNotExistsText}");
            }

            writer.WriteSql($" {Name}");

            if (Location != null)
            {
                writer.WriteSql($" LOCATION '{Location}'");
            }

            if (ManagedLocation != null)
            {
                // ReSharper disable once StringLiteralTypo
                writer.WriteSql($" MANAGEDLOCATION '{ManagedLocation}'");
            }
        }
    }
    /// <summary>
    /// Create function statement
    ///
    /// Supported variants:
    /// Hive <see href="https://cwiki.apache.org/confluence/display/hive/languagemanual+ddl#LanguageManualDDL-Create/Drop/ReloadFunction"/>
    /// Postgres <see href="https://www.postgresql.org/docs/15/sql-createfunction.html"/>
    /// </summary>
    /// <param name="Name">Function name</param>
    public record CreateFunction([Visit(0)] ObjectName Name, [Visit(1)] CreateFunctionBody Parameters) : Statement
    {
        public bool OrReplace { get; init; }
        public bool Temporary { get; init; }
        [Visit(2)] public Sequence<OperateFunctionArg>? Args { get; init; }
        public DataType? ReturnType { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var or = OrReplace ? "OR REPLACE " : null;
            var temp = Temporary ? "TEMPORARY " : null;
            writer.WriteSql($"CREATE {or}{temp}FUNCTION {Name}");

            if (Args.SafeAny())
            {
                writer.WriteSql($"({Args})");
            }

            if (ReturnType != null)
            {
                writer.WriteSql($" RETURNS {ReturnType}");
            }

            writer.WriteSql($"{Parameters}");
        }
    }
    /// <summary>
    /// Create Index statement
    /// </summary>
    public record CreateIndex([property: Visit(0)] ObjectName Name, [property: Visit(1)] ObjectName TableName) : Statement, IIfNotExists
    {
        public Ident? Using { get; init; }
        [Visit(2)] public Sequence<OrderByExpression>? Columns { get; init; }
        public bool Unique { get; init; }
        public bool IfNotExists { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var unique = Unique ? "UNIQUE " : null;
            var ifNot = IfNotExists ? $"{AsIne.IfNotExistsText} " : null;
            writer.WriteSql($"CREATE {unique}INDEX {ifNot}{Name} ON {TableName}");

            if (Using != null)
            {
                writer.WriteSql($" USING {Using} ");
            }

            writer.WriteSql($"({Columns})");
        }
    }
    /// <summary>
    /// Create stage statement
    /// <remarks>
    ///  <see href="https://docs.snowflake.com/en/sql-reference/sql/create-stage"/> 
    /// </remarks>
    /// </summary>
    public record CreateStage([property: Visit(0)] ObjectName Name, [property: Visit(1)] StageParams StageParams) : Statement, IIfNotExists
    {
        public bool OrReplace { get; init; }
        public bool Temporary { get; init; }
        public bool IfNotExists { get; init; }
        public Sequence<DataLoadingOption>? DirectoryTableParams { get; init; }
        public Sequence<DataLoadingOption>? FileFormat { get; init; }
        public Sequence<DataLoadingOption>? CopyOptions { get; init; }
        public string? Comment { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var orReplace = OrReplace ? "OR REPLACE " : null;
            var temp = Temporary ? "TEMPORARY " : null;
            var ifNot = IfNotExists ? $"{AsIne.IfNotExistsText} " : null;

            writer.WriteSql($"CREATE {orReplace}{temp}STAGE {ifNot}{Name}{StageParams}");

            if (DirectoryTableParams.SafeAny())
            {
                writer.WriteSql($" DIRECTORY=({DirectoryTableParams.ToSqlDelimited(" ")})");
            }
            if (FileFormat.SafeAny())
            {
                writer.WriteSql($" FILE_FORMAT=({FileFormat.ToSqlDelimited(" ")})");
            }
            if (CopyOptions.SafeAny())
            {
                writer.WriteSql($" COPY_OPTIONS=({CopyOptions.ToSqlDelimited(" ")})");
            }
            if (Comment != null)
            {
                writer.WriteSql($" COMMENT='{Comment}'");
            }
        }
    }
    /// <summary>
    /// Create Table statement
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="Columns">Table columns</param>
    public record CreateTable([property: Visit(0)] ObjectName Name, [property: Visit(1)] Sequence<ColumnDef> Columns) : Statement, IIfNotExists
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
        public Sequence<Ident>? OrderBy { get; init; }
        public string? DefaultCharset { get; init; }
        public string? Collation { get; init; }
        public OnCommit OnCommit { get; init; }
        // Clickhouse "ON CLUSTER" clause:
        // https://clickhouse.com/docs/en/sql-reference/distributed-ddl/
        public string? OnCluster { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var orReplace = OrReplace ? "OR REPLACE " : null;
            var external = External ? "EXTERNAL " : null;
            var global = Global.HasValue ? Global.Value ? "GLOBAL " : "LOCAL " : null;
            var temp = Temporary ? "TEMPORARY " : null;
            var transient = Transient ? "TRANSIENT " : null;
            var ifNot = IfNotExists ?$"{AsIne.IfNotExistsText} " : null;
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
                    case HiveRowFormat.Delimited:
                        writer.WriteSql($" ROW FORMAT DELIMITED");
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

            if (OrderBy.SafeAny())
            {
                writer.WriteSql($" ORDER BY ({OrderBy})");
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
        }
    }
    /// <summary>
    /// Create View statement
    /// </summary>
    /// <param name="Name">Object name</param>
    public record CreateView([property: Visit(0)] ObjectName Name, [property: Visit(1)] Select Query) : Statement
    {
        public bool OrReplace { get; init; }
        public bool Materialized { get; init; }
        public Sequence<Ident>? Columns { get; init; }
        [Visit(2)] public Sequence<SqlOption>? WithOptions { get; init; }
        public Sequence<Ident>? ClusterBy { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var orReplace = OrReplace ? "OR REPLACE " : null;
            var materialized = Materialized ? "MATERIALIZED " : null;
            writer.WriteSql($"CREATE {orReplace}{materialized}VIEW {Name}");

            if (WithOptions.SafeAny())
            {
                writer.WriteSql($" WITH ({WithOptions!.ToSqlDelimited()})");
            }

            if (Columns.SafeAny())
            {
                writer.WriteSql($" ({Columns!.ToSqlDelimited()})");
            }

            if (ClusterBy.SafeAny())
            {
                writer.WriteSql($" CLUSTER BY ({ClusterBy!.ToSqlDelimited()})");
            }

            writer.Write(" AS ");
            Query.ToSql(writer);
        }
    }
    /// <summary>
    /// SQLite's CREATE VIRTUAL TABLE .. USING module_name (module_args)
    /// </summary>
    /// <param name="Name">Virtual table name</param>
    public record CreateVirtualTable(ObjectName Name) : Statement, IIfNotExists
    {
        public bool IfNotExists { get; init; }
        public Ident? ModuleName { get; init; }
        public Sequence<Ident>? ModuleArgs { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var ifNot = IfNotExists ? $"{AsIne.IfNotExistsText} " : null;
            writer.WriteSql($"CREATE VIRTUAL TABLE {ifNot}{Name} USING {ModuleName}");

            if (ModuleArgs.SafeAny())
            {
                writer.WriteSql($" ({ModuleArgs!.ToSqlDelimited()})");
            }
        }
    }
    /// <summary>
    /// CREATE ROLE statement
    /// postgres - <see href="https://www.postgresql.org/docs/current/sql-createrole.html"/>
    /// </summary>
    public record CreateRole([property: Visit(0)] Sequence<ObjectName> Names) : Statement, IIfNotExists
    {
        public bool IfNotExists { get; init; }
        // Postgres
        public bool? Login { get; init; }
        public bool? Inherit { get; init; }
        public bool? BypassRls { get; init; }
        public Password? Password { get; init; }
        public bool? Superuser { get; init; }
        public bool? CreateDb { get; init; }
        public bool? CreateDbRole { get; init; }
        public bool? Replication { get; init; }
        [Visit(1)] public Expression? ConnectionLimit { get; init; }
        [Visit(2)] public Expression? ValidUntil { get; init; }
        public Sequence<Ident>? InRole { get; init; }
        public Sequence<Ident>? InGroup { get; init; }
        public Sequence<Ident>? User { get; init; }
        public Sequence<Ident>? Role { get; init; }
        public Sequence<Ident>? Admin { get; init; }
        // MSSQL
        [Visit(3)] public ObjectName? AuthorizationOwner { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var superuser = Superuser.HasValue ? Superuser.Value ? " SUPERUSER" : " NOSUPERUSER" : null;
            var createDb = CreateDb.HasValue ? CreateDb.Value ? " CREATEDB" : " NOCREATEDB" : null;
            var createRole = CreateDbRole.HasValue ? CreateDbRole.Value ? " CREATEROLE" : " NOCREATEROLE" : null;
            var inherit = Inherit.HasValue ? Inherit.Value ? " INHERIT" : " NOINHERIT" : null;
            var login = Login.HasValue ? Login.Value ? " LOGIN" : " NOLOGIN" : null;
            var replication = Replication.HasValue ? Replication.Value ? " REPLICATION" : " NOREPLICATION" : null;
            var bypassrls = BypassRls.HasValue ? BypassRls.Value ? " BYPASSRLS" : " NOBYPASSRLS" : null;
            var ifNot = IfNotExists ? $"{AsIne.IfNotExistsText} " : null;
            writer.WriteSql($"CREATE ROLE {ifNot}{Names}{superuser}{createDb}{createRole}{inherit}{login}{replication}{bypassrls}");

            if (ConnectionLimit != null)
            {
                writer.WriteSql($" CONNECTION LIMIT {ConnectionLimit.ToSql()}");
            }

            if (Password != null)
            {
                switch (Password)
                {
                    case Password.ValidPassword vp:
                        writer.WriteSql($" PASSWORD {vp.Expression.ToSql()}");
                        break;
                    case Password.NullPassword:
                        writer.Write(" PASSWORD NULL");
                        break;
                }
            }

            if (ValidUntil != null)
            {
                writer.WriteSql($" VALID UNTIL {ValidUntil.ToSql()}");
            }

            if (InRole.SafeAny())
            {
                writer.WriteSql($" IN ROLE {InRole.ToSqlDelimited()}");
            }

            if (InGroup.SafeAny())
            {
                writer.WriteSql($" IN GROUP {InGroup.ToSqlDelimited()}");
            }

            if (Role.SafeAny())
            {
                writer.WriteSql($" ROLE {Role.ToSqlDelimited()}");
            }

            if (User.SafeAny())
            {
                writer.WriteSql($" USER {User.ToSqlDelimited()}");
            }

            if (Admin.SafeAny())
            {
                writer.WriteSql($" ADMIN {Admin.ToSqlDelimited()}");
            }

            if (AuthorizationOwner != null)
            {
                writer.WriteSql($" AUTHORIZATION {AuthorizationOwner.ToSql()}");
            }
        }
    }
    /// <summary>
    /// CREATE SCHEMA statement
    /// <example>
    /// <c>
    /// schema_name | AUTHORIZATION schema_authorization_identifier | schema_name  AUTHORIZATION schema_authorization_identifier
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name">Schema name</param>
    /// <param name="IfNotExists">True for if not exists</param>
    public record CreateSchema(SchemaName Name, bool IfNotExists) : Statement, IIfNotExists
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var ifNot = IfNotExists ? $"{AsIne.IfNotExistsText} " : null;
            writer.WriteSql($"CREATE SCHEMA {ifNot}{Name}");
        }
    }
    /// <summary>
    /// CREATE SCHEMA statement
    /// <example>
    /// <c>
    /// CREATE [ { TEMPORARY | TEMP } ] SEQUENCE [ IF NOT EXISTS ] sequence_name
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name">Schema name</param>
    public record CreateSequence([property: Visit(0)]ObjectName Name) : Statement, IIfNotExists
    {
        public bool Temporary { get; init; }
        public bool IfNotExists { get; init; }
        public DataType? DataType { get; init; }
        [Visit(1)] public Sequence<SequenceOptions>? SequenceOptions { get; init; }
        [Visit(2)] public ObjectName? OwnedBy { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var asType = DataType != null ? $" AS {DataType.ToSql()}" : null;
            var temp = Temporary ? "TEMPORARY " : null;
            var ifNot = IfNotExists ? $"{AsIne.IfNotExistsText} " : null;
            writer.Write($"CREATE {temp}SEQUENCE {ifNot}{Name}{asType}");

            if (SequenceOptions != null)
            {
                foreach (var option in SequenceOptions)
                {
                    writer.WriteSql($"{option}");
                }
            }

            if (OwnedBy != null)
            {
                writer.WriteSql($" OWNED BY {OwnedBy}");
            }
        }
    }
    /// <summary>
    /// DEALLOCATE statement
    /// </summary>
    /// <param name="Name">Name identifier</param>
    public record Deallocate(Ident Name, bool Prepared) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var prepare = Prepared ? "PREPARE " : null;
            writer.WriteSql($"DEALLOCATE {prepare}{Name}");
        }
    }
    /// <summary>
    /// Execute statement
    /// </summary>
    /// <param name="Name">Name identifier</param>
    /// <param name="Parameters">Parameter expressions</param>
    public record Execute(Ident Name, Sequence<Expression>? Parameters = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"EXECUTE {Name}");

            if (Parameters.SafeAny())
            {
                writer.WriteSql($"({Parameters})");
            }
        }
    }
    /// <summary>
    /// DROP statement
    /// </summary>
    /// <param name="Names">Object names</param>
    public record Drop(Sequence<ObjectName> Names) : Statement
    {
        /// The type of the object to drop: TABLE, VIEW, etc.
        public ObjectType ObjectType { get; init; }
        /// An optional `IF EXISTS` clause. (Non-standard.)
        public bool IfExists { get; init; }
        /// Whether `CASCADE` was specified. This will be `false` when
        /// `RESTRICT` or no drop behavior at all was specified.
        public bool Cascade { get; init; }
        /// Whether `RESTRICT` was specified. This will be `false` when
        /// `CASCADE` or no drop behavior at all was specified.
        public bool Restrict { get; init; }
        /// Hive allows you specify whether the table's stored data will be
        /// deleted along with the dropped table
        public bool Purge { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var ifExists = IfExists ? " IF EXISTS" : null;
            var cascade = Cascade ? " CASCADE" : null;
            var restrict = Restrict ? " RESTRICT" : null;
            var purge = Purge ? " PURGE" : null;

            writer.WriteSql($"DROP {ObjectType}{ifExists} {Names}{cascade}{restrict}{purge}");
        }
    }
    /// <summary>
    /// DROP Function statement
    /// </summary>
    /// <param name="IfExists">True if exists</param>
    /// <param name="FuncDesc">Drop function descriptions</param>
    /// <param name="Option">Referential actions</param>
    public record DropFunction(bool IfExists, Sequence<DropFunctionDesc> FuncDesc, ReferentialAction Option) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var ifEx = IfExists ? " IF EXISTS" : null;
            writer.WriteSql($"DROP FUNCTION{ifEx} {FuncDesc}");

            if (Option != ReferentialAction.None)
            {
                writer.Write($" {Option}");
            }
        }
    }
    /// <summary>
    /// Drop function description
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="Args">Operate function arguments</param>
    public record DropFunctionDesc(ObjectName Name, Sequence<OperateFunctionArg>? Args = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Name.ToSql(writer);
            if (Args.SafeAny())
            {
                writer.WriteSql($"({Args})");
            }
        }
    }
    /// <summary>
    /// DISCARD [ ALL | PLANS | SEQUENCES | TEMPORARY | TEMP ]
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    /// </summary>
    /// <param name="ObjectType">Discard object type</param>
    public record Discard(DiscardObject ObjectType) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"DISCARD {ObjectType}");
        }
    }
    /// <summary>
    /// DECLARE - Declaring Cursor Variables
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    /// </summary>
    /// <param name="Name">Name identifier</param>
    public record Declare(Ident Name) : Statement
    {
        /// <summary>
        /// Causes the cursor to return data in binary rather than in text format.
        /// </summary>
        public bool? Binary { get; init; }
        /// <summary>
        /// None = Not specified
        /// Some(true) = INSENSITIVE
        /// Some(false) = ASENSITIVE
        /// </summary>
        public bool? Sensitive { get; init; }
        /// <summary>
        /// None = Not specified
        /// Some(true) = SCROLL
        /// Some(false) = NO SCROLL
        /// </summary>
        public bool? Scroll { get; init; }
        /// <summary>
        /// None = Not specified
        /// Some(true) = WITH HOLD, specifies that the cursor can continue to be used after the transaction that created it successfully commits
        /// Some(false) = WITHOUT HOLD, specifies that the cursor cannot be used outside of the transaction that created it
        /// </summary>
        public bool? Hold { get; init; }
        /// <summary>
        /// Select
        /// </summary>
        public Select? Query { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"DECLARE {Name} ");
            if (Binary.HasValue && Binary.Value)
            {
                writer.Write("BINARY ");
            }

            if (Sensitive.HasValue)
            {
                writer.Write(Sensitive.Value ? "INSENSITIVE " : "ASENSITIVE ");
            }

            if (Scroll.HasValue)
            {
                writer.Write(Scroll.Value ? "SCROLL " : "NO SCROLL ");
            }

            writer.Write("CURSOR ");

            if (Hold.HasValue)
            {
                writer.Write(Hold.Value ? "WITH HOLD " : "WITHOUT HOLD ");
            }

            writer.WriteSql($"FOR {Query}");
        }
    }
    /// <summary>
    /// Delete statement
    /// </summary>
    /// <param name="Name">Table name</param>
    /// <param name="Using">Using</param>
    /// <param name="Selection">Selection expression</param>
    /// <param name="Returning">Select items to return</param>
    public record Delete(TableFactor Name, TableFactor? Using = null, Expression? Selection = null, Sequence<SelectItem>? Returning = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DELETE FROM {Name}");
            if (Using != null)
            {
                writer.WriteSql($" USING {Using}");
            }

            if (Selection != null)
            {
                writer.WriteSql($" WHERE {Selection}");
            }

            if (Returning != null)
            {
                writer.WriteSql($" RETURNING {Returning}");
            }
        }
    }
    /// <summary>
    /// Directory statement
    /// </summary>
    /// <param name="Overwrite">True if overwrite</param>
    /// <param name="Local">True if local</param>
    /// <param name="Path">Path</param>
    /// <param name="FileFormat">File format</param>
    /// <param name="Source">Source query</param>
    public record Directory(bool Overwrite, bool Local, string? Path, FileFormat FileFormat, Select Source) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var overwrite = Overwrite ? " OVERWRITE" : null;
            var local = Local ? " LOCAL" : null;
            writer.WriteSql($"INSERT{overwrite}{local} DIRECTORY '{Path}'");

            if (FileFormat != FileFormat.None)
            {
                writer.WriteSql($" STORED AS {FileFormat}");
            }

            writer.WriteSql($" {Source}");
        }
    }
    /// <summary>
    /// EXPLAIN / DESCRIBE statement
    /// </summary>
    public record Explain(Statement Statement) : Statement
    {
        // ReSharper disable once MemberHidesStaticFromOuterClass
        public new bool Analyze { get; set; }

        // If true, query used the MySQL `DESCRIBE` alias for explain
        public bool DescribeAlias { get; init; }

        // Display additional information regarding the plan.
        public bool Verbose { get; init; }

        /// Optional output format of explain
        public AnalyzeFormat Format { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(DescribeAlias ? "DESCRIBE " : "EXPLAIN ");

            if (Analyze)
            {
                writer.Write("ANALYZE ");
            }

            if (Verbose)
            {
                writer.Write("VERBOSE ");
            }

            if (Format != AnalyzeFormat.None)
            {
                writer.WriteSql($"FORMAT {Format} ");
            }

            Statement.ToSql(writer);
        }
    }
    /// <summary>
    /// EXPLAIN TABLE
    /// Note: this is a MySQL-specific statement. <see href="https://dev.mysql.com/doc/refman/8.0/en/explain.html"/>
    /// </summary>
    /// <param name="DescribeAlias">If true, query used the MySQL DESCRIBE alias for explain</param>
    /// <param name="Name">Table name</param>
    public record ExplainTable(bool DescribeAlias, ObjectName Name) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(DescribeAlias ? "DESCRIBE " : "EXPLAIN ");
            writer.Write(Name);
        }
    }
    /// <summary>
    /// FETCH - retrieve rows from a query using a cursor
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    /// </summary>
    /// <param name="Name">Name identifier</param>
    /// <param name="FetchDirection">Fetch direction</param>
    /// <param name="Into">Fetch into name</param>
    public record Fetch(Ident Name, FetchDirection FetchDirection, ObjectName? Into = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"FETCH {FetchDirection} ");
            writer.WriteSql($"IN {Name}");

            if (Into != null)
            {
                writer.WriteSql($" INTO {Into}");
            }
        }
    }
    /// <summary>
    /// GRANT privileges ON objects TO grantees
    /// </summary>
    /// <param name="Privileges">Privileges</param>
    /// <param name="Objects">Grant Objects</param>
    /// <param name="Grantees">Grantees</param>
    /// <param name="WithGrantOption">WithGrantOption</param>
    /// <param name="GrantedBy">Granted by name</param>
    public record Grant(Privileges Privileges, GrantObjects? Objects, Sequence<Ident> Grantees, bool WithGrantOption, Ident? GrantedBy = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"GRANT {Privileges} ");
            writer.WriteSql($"ON {Objects} ");
            writer.WriteSql($"TO {Grantees}");

            if (WithGrantOption)
            {
                writer.Write(" WITH GRANT OPTION");
            }

            if (GrantedBy != null)
            {
                writer.WriteSql($" GRANTED BY {GrantedBy}");
            }
        }
    }
    /// <summary>
    /// Insert statement
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="Source">Source query</param>
    public record Insert([property: Visit(0)] ObjectName Name, [property: Visit(1)] Select Source) : Statement
    {
        /// Only for Sqlite
        public SqliteOnConflict Or { get; init; }
        /// INTO - optional keyword
        public bool Into { get; init; }
        /// COLUMNS
        public Sequence<Ident>? Columns { get; init; }
        /// Overwrite (Hive)
        public bool Overwrite { get; init; }
        /// partitioned insert (Hive)
        [Visit(2)] public Sequence<Expression>? Partitioned { get; init; }
        /// Columns defined after PARTITION
        public Sequence<Ident>? AfterColumns { get; init; }
        /// whether the insert has the table keyword (Hive)
        public bool Table { get; init; }
        public OnInsert? On { get; init; }
        /// RETURNING
        [Visit(3)] public Sequence<SelectItem>? Returning { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            if (Or != SqliteOnConflict.None)
            {
                writer.WriteSql($"INSERT OR {Or} INTO {Name} ");
            }
            else
            {
                var over = Overwrite ? " OVERWRITE" : null;
                var into = Into ? " INTO" : null;
                var table = Table ? " TABLE" : null;
                writer.Write($"INSERT{over}{into}{table} {Name} ");
            }

            if (Columns.SafeAny())
            {
                writer.WriteSql($"({Columns}) ");
            }

            if (Partitioned.SafeAny())
            {
                writer.WriteSql($"PARTITION ({Partitioned}) ");
            }

            if (AfterColumns.SafeAny())
            {
                writer.WriteSql($"({AfterColumns}) ");
            }

            Source.ToSql(writer);

            if (On != null)
            {
                On.ToSql(writer);
            }

            if (Returning.SafeAny())
            {
                writer.WriteSql($" RETURNING {Returning}");
            }
        }
    }
    /// <summary>
    /// KILL [CONNECTION | QUERY | MUTATION]
    ///
    /// <see href="https://clickhouse.com/docs/ru/sql-reference/statements/kill/"/>
    /// <see href="https://dev.mysql.com/doc/refman/8.0/en/kill.html"/>
    /// </summary>
    /// <param name="Modifier">KillType modifier</param>
    /// <param name="Id">Id value</param>
    public record Kill(KillType Modifier, ulong Id) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("KILL ");

            if (Modifier != KillType.None)
            {
                writer.WriteSql($"{Modifier} ");
            }

            writer.Write(Id);
        }
    }
    /// <summary>
    /// Merge statement
    /// </summary>
    /// <param name="Into">True if into</param>
    /// <param name="Table">Table</param>
    /// <param name="Source">Source table factor</param>
    /// <param name="On">ON expression</param>
    /// <param name="Clauses">Merge Clauses</param>
    public record Merge(bool Into, TableFactor Table, TableFactor Source, Expression On, Sequence<MergeClause> Clauses) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var into = Into ? " INTO" : null;
            writer.WriteSql($"MERGE{into} {Table} USING {Source} ON {On} {Clauses.ToSqlDelimited(" ")}");
        }
    }
    /// <summary>
    /// Msck (Hive)
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="Repair">Repair</param>
    /// <param name="PartitionAction">Partition action</param>
    // ReSharper disable once IdentifierTypo
    public record Msck(ObjectName Name, bool Repair, AddDropSync PartitionAction) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var repair = Repair ? "REPAIR " : null;
            writer.WriteSql($"MSCK {repair}TABLE {Name}");

            if (PartitionAction != AddDropSync.None)
            {
                writer.WriteSql($" {PartitionAction}");
            }
        }
    }
    /// <summary>
    ///Prepare statement
    /// <example>
    /// <c>
    /// `PREPARE name [ ( data_type [, ...] ) ] AS statement`
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name">Name identifier</param>
    /// <param name="DataTypes">Data types</param>
    /// <param name="Statement">Statement</param>
    ///
    /// Note: this is a PostgreSQL-specific statement.
    public record Prepare(Ident Name, Sequence<DataType> DataTypes, Statement Statement) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"PREPARE {Name} ");
            if (DataTypes.SafeAny())
            {
                writer.WriteSql($"({DataTypes}) ");
            }

            writer.WriteSql($"AS {Statement}");
        }
    }
    /// <summary>
    /// Select statement
    /// </summary>
    /// <param name="Query">Select query</param>
    public record Select(Query Query) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Query.ToSql(writer);
        }
    }
    /// <summary>
    /// Revoke statement
    /// </summary>
    /// <param name="Privileges">Privileges</param>
    /// <param name="Objects">Grant Objects</param>
    /// <param name="Grantees">Grantees</param>
    /// <param name="GrantedBy">Granted by name</param>
    /// <param name="Cascade">Cascade</param>
    public record Revoke(Privileges Privileges, GrantObjects Objects, Sequence<Ident> Grantees, bool Cascade = false, Ident? GrantedBy = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"REVOKE {Privileges} ");
            writer.WriteSql($"ON {Objects} ");
            writer.WriteSql($"FROM {Grantees}");

            if (GrantedBy != null)
            {
                writer.WriteSql($" GRANTED BY {GrantedBy}");
            }

            writer.Write(Cascade ? " CASCADE" : " RESTRICT");
        }
    }
    /// <summary>
    /// Rollback statement
    /// </summary>
    /// <param name="Chain">True if chaining</param>
    public record Rollback(bool Chain) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var chain = Chain ? " AND CHAIN" : null;
            writer.Write($"ROLLBACK{chain}");
        }
    }
    /// <summary>
    /// Savepoint statement
    /// </summary>
    /// <param name="Name">Name identifier</param>
    public record Savepoint(Ident Name) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SAVEPOINT {Name}");
        }
    }
    /// <summary>
    /// SET NAMES 'charset_name' [COLLATE 'collation_name']
    /// 
    /// Note: this is a MySQL-specific statement.
    /// </summary>
    /// <param name="CharsetName">Character set name</param>
    /// <param name="CollationName">Collation name</param>
    public record SetNames(string CharsetName, string? CollationName = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"SET NAMES {CharsetName}");

            if (CollationName != null)
            {
                writer.Write($" COLLATE {CollationName}");
            }
        }
    }
    /// <summary>
    /// SET NAMES DEFAULT
    /// Note: this is a MySQL-specific statement.
    /// </summary>
    public record SetNamesDefault : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SET NAMES DEFAULT");
        }
    }
    /// <summary>
    /// SET [ SESSION | LOCAL ] ROLE role_name. Examples: ANSI, Postgresql, MySQL, and Oracle.
    /// </summary>
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#set-role-statement"/>
    /// <see href="https://www.postgresql.org/docs/14/sql-set-role.html"/>
    /// <see href="https://dev.mysql.com/doc/refman/8.0/en/set-role.html"/>
    /// <see href="https://docs.oracle.com/cd/B19306_01/server.102/b14200/statements_10004.htm"/>
    ///
    /// <param name="ContextModifier">Context modifier flag</param>
    /// <param name="Name">Name identifier</param>
    public record SetRole(ContextModifier ContextModifier, Ident? Name = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var context = ContextModifier switch
            {
                ContextModifier.Local => " LOCAL",
                ContextModifier.Session => " SESSION",
                _ => null
            };

            writer.WriteSql($"SET{context} ROLE {Name ?? "NONE"}");
        }
    }
    /// <summary>
    /// SET TIME ZONE value
    /// Note: this is a PostgreSQL-specific statements
    ///`SET TIME ZONE value is an alias for SET timezone TO value in PostgreSQL
    /// </summary>
    /// <param name="Local">True if local</param>
    /// <param name="Value">Expression value</param>
    public record SetTimeZone(bool Local, Expression Value) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SET ");

            if (Local)
            {
                writer.Write("LOCAL ");
            }

            writer.WriteSql($"TIME ZONE {Value}");
        }
    }
    /// <summary>
    /// SET TRANSACTION
    /// </summary>
    /// <param name="Modes">Transaction modes</param>
    /// <param name="Snapshot">Snapshot value</param>
    /// <param name="Session">True if using session</param>
    public record SetTransaction(Sequence<TransactionMode>? Modes, Value? Snapshot = null, bool Session = false) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(Session
                ? "SET SESSION CHARACTERISTICS AS TRANSACTION"
                : "SET TRANSACTION");

            if (Modes.SafeAny())
            {
                writer.WriteSql($" {Modes}");
            }

            if (Snapshot != null)
            {
                writer.WriteSql($" SNAPSHOT {Snapshot}");
            }
        }
    }
    /// <summary>
    /// SET variable
    ///
    /// Note: this is not a standard SQL statement, but it is supported by at
    /// least MySQL and PostgreSQL. Not all MySQL-specific syntactic forms are
    /// SET variable
    /// </summary>
    /// <param name="Local">True if local</param>
    /// <param name="HiveVar">True if Hive variable</param>
    /// <param name="Variable">Variable name</param>
    /// <param name="Value">Value</param>
    public record SetVariable(bool Local, bool HiveVar, ObjectName? Variable = null, Sequence<Expression>? Value = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SET ");

            if (Local)
            {
                writer.Write("LOCAL ");
            }

            var hiveVar = HiveVar ? "HIVEVAR:" : null;

            writer.WriteSql($"{hiveVar}{Variable} = {Value}");
        }
    }
    /// <summary>
    /// Show Collation statement
    /// </summary>
    /// <param name="Filter">Filter</param>
    public record ShowCollation(ShowStatementFilter? Filter = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SHOW COLLATION");

            if (Filter != null)
            {
                writer.WriteSql($" {Filter}");
            }
        }
    }
    /// <summary>
    /// SHOW COLUMNS
    /// 
    /// Note: this is a MySQL-specific statement.
    /// </summary>
    /// <param name="Extended">True if extended</param>
    /// <param name="Full">True if full</param>
    /// <param name="TableName"></param>
    /// <param name="Filter"></param>
    public record ShowColumns(bool Extended, bool Full, ObjectName? TableName = null, ShowStatementFilter? Filter = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var extended = Extended ? "EXTENDED " : null;
            var full = Full ? "FULL " : null;

            writer.WriteSql($"SHOW {extended}{full}COLUMNS FROM {TableName}");

            if (Filter != null)
            {
                writer.WriteSql($" {Filter}");
            }
        }
    }
    /// <summary>
    /// SHOW CREATE TABLE
    ///
    /// Note: this is a MySQL-specific statement.
    /// </summary>
    /// <param name="ObjectType">Show Create Object</param>
    /// <param name="ObjectName">Object name</param>
    public record ShowCreate(ShowCreateObject ObjectType, ObjectName ObjectName) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SHOW CREATE {ObjectType} {ObjectName}");
        }
    }
    /// <summary>
    /// SHOW VARIABLE
    /// </summary>
    /// <param name="Variable">Variable identifiers</param>
    public record ShowVariable(Sequence<Ident> Variable) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SHOW");

            if (Variable.SafeAny())
            {
                writer.WriteSql($" {Variable.ToSqlDelimited(" ")}");
            }
        }
    }
    /// <summary>
    /// SHOW VARIABLES
    /// </summary>
    /// <param name="Filter">Show statement filter</param>
    public record ShowVariables(ShowStatementFilter? Filter = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SHOW VARIABLES");

            if (Filter != null)
            {
                writer.WriteSql($" {Filter}");
            }
        }
    }
    /// <summary>
    /// SHOW FUNCTIONS
    /// </summary>
    /// <param name="Filter">Show statement filter</param>
    public record ShowFunctions(ShowStatementFilter? Filter = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SHOW FUNCTIONS");

            if (Filter != null)
            {
                writer.WriteSql($" {Filter}");
            }
        }
    }
    /// <summary>
    /// SHOW TABLES
    /// </summary>
    /// <param name="Extended">True if extended</param>
    /// <param name="Full">True if full</param>
    /// <param name="Name">Optional database name</param>
    /// <param name="Filter">Optional filter</param>
    public record ShowTables(bool Extended, bool Full, Ident? Name = null, ShowStatementFilter? Filter = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var extended = Extended ? "EXTENDED " : null;
            var full = Full ? "FULL " : null;
            writer.Write($"SHOW {extended}{full}TABLES");

            if (Name != null)
            {
                writer.WriteSql($" FROM {Name}");
            }

            if (Filter != null)
            {
                writer.WriteSql($" {Filter}");
            }
        }
    }
    /// <summary>
    /// START TRANSACTION
    /// </summary>
    /// <param name="Modes">Transaction modes</param>
    public record StartTransaction(Sequence<TransactionMode>? Modes) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("START TRANSACTION");

            if (Modes.SafeAny())
            {
                writer.WriteSql($" {Modes}");
            }
        }
    }
    /// <summary>
    /// Truncate (Hive)
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="Partitions">List of partitions</param>
    public record Truncate(ObjectName Name, Sequence<Expression>? Partitions) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"TRUNCATE TABLE {Name}");

            if (Partitions.SafeAny())
            {
                writer.WriteSql($" PARTITION ({Partitions})");
            }
        }
    }
    /// <summary>
    /// UNCACHE TABLE [ IF EXISTS ]  table_name
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="IfExists">True if exists statement</param>
    // ReSharper disable once InconsistentNaming
    public record UNCache(ObjectName Name, bool IfExists = false) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(IfExists 
                ? $"UNCACHE TABLE IF EXISTS {Name}" 
                : $"UNCACHE TABLE {Name}");
        }
    }
    /// <summary>
    /// Update statement
    /// </summary>
    /// <param name="Table">Table with joins to update</param>
    /// <param name="Assignments">Assignments</param>
    /// <param name="From">Update source</param>
    /// <param name="Selection">Selection expression</param>
    /// <param name="Returning">Select returning values</param>
    public record Update(TableWithJoins Table, Sequence<Assignment> Assignments, TableWithJoins? From = null, Expression? Selection = null, Sequence<SelectItem>? Returning = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"UPDATE {Table}");

            if (Assignments.SafeAny())
            {
                writer.WriteSql($" SET {Assignments}");
            }

            if (From != null)
            {
                writer.WriteSql($" FROM {From}");
            }

            if (Selection != null)
            {
                writer.WriteSql($" WHERE {Selection}");
            }

            if (Returning != null)
            {
                writer.WriteSql($" RETURNING {Returning}");
            }
        }
    }
    /// <summary>
    /// USE statement
    ///
    /// Note: This is a MySQL-specific statement.
    /// </summary>
    /// <param name="Name">Name identifier</param>
    public record Use(Ident Name) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"USE {Name}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);

    internal IIfNotExists AsIne => (IIfNotExists)this;

    public T As<T>() where T : Statement
    {
        return (T)this;
    }
    public Query? AsQuery()
    {
        if (this is Select select)
        {
            return (Query)select;
        }

        return null;
    }

    public Select AsSelect()
    {
        return As<Select>();
    }

    public Insert AsInsert()
    {
        return As<Insert>();
    }

    public Update AsUpdate()
    {
        return As<Update>();
    }

    public Delete AsDelete()
    {
        return As<Delete>();
    }
}