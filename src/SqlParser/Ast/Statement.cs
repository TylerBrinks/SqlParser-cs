// ReSharper disable StringLiteralTypo
// ReSharper disable CommentTypo
// ReSharper disable UnusedMember

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
    public record AlterTable(
        ObjectName Name,
        bool IfExists,
        bool Only,
        Sequence<AlterTableOperation> Operations,
        HiveSetLocation? Location,
        Ident? OnCluster = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("ALTER TABLE ");

            if (IfExists)
            {
                writer.Write("IF EXISTS ");
            }
            if (Only)
            {
                writer.Write("ONLY ");
            }

            writer.WriteSql($"{Name} ");

            if (OnCluster != null)
            {
                writer.WriteSql($"ON CLUSTER {OnCluster} ");
            }

            writer.WriteDelimited(Operations);

            if (Location != null)
            {
                writer.WriteSql($" {Location}");
            }
        }
    }
    /// <summary>
    /// Alter view statement
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="Columns">Columns</param>
    /// <param name="Query">Alter query</param>
    /// <param name="WithOptions">With options</param>
    public record AlterView(ObjectName Name, Sequence<Ident> Columns, Query Query, Sequence<SqlOption> WithOptions) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ALTER VIEW {Name}");

            if (WithOptions.SafeAny())
            {
                writer.Write($" WITH ({WithOptions.ToSqlDelimited()})");
            }

            if (Columns.SafeAny())
            {
                writer.Write($" ({Columns.ToSqlDelimited()})");
            }

            writer.WriteSql($" AS {Query}");
        }
    }
    /// <summary>
    /// Alter role statement
    /// </summary>
    public record AlterRole(Ident Name, AlterRoleOperation Operation) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ALTER ROLE {Name} {Operation}");
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
    /// <param name="Target">Assignment target</param>
    /// <param name="Value">Expression value</param>
    public record Assignment(AssignmentTarget Target, Expression Value) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Target} = {Value}");
        }
    }
    /// <summary>
    /// ATTACH DATABASE 'path/to/file' AS alias (SQLite-specific)
    /// </summary>
    /// <param name="SchemaName">Schema name</param>
    /// <param name="DatabaseFileName">Database file name</param>
    /// <param name="Database">True if database; otherwise false</param>
    public record AttachDatabase(Ident SchemaName, Expression DatabaseFileName, bool Database) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var keyword = Database ? "DATABASE " : "";
            writer.WriteSql($"ATTACH {keyword}{DatabaseFileName} AS {SchemaName}");
        }
    }

    public record AttachDuckDbDatabase(
        bool IfNotExists,
        bool Database,
        Ident DatabasePath,
        Ident? DatabaseAlias,
        Sequence<AttachDuckDbDatabaseOption>? AttachOption) : Statement, IIfNotExists
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var ifNot = IfNotExists ? $" {IIfNotExists.IfNotExistsPhrase} " : null;
            var keyword = Database ? "DATABASE" : "";
            writer.WriteSql($"ATTACH {keyword}{ifNot}{DatabasePath}");

            if (DatabaseAlias != null)
            {
                writer.WriteSql($" AS {DatabaseAlias}");
            }

            if (AttachOption.SafeAny())
            {
                writer.Write($" ({AttachOption.ToSqlDelimited()})");
            }
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
    /// Call statement
    /// </summary>
    public record Call(Expression.Function Function) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"CALL {Function}");
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
    /// <param name="Source">Source of the Coyp To</param>
    /// <param name="To">True if to</param>
    /// <param name="Target">Copy target</param>
    public record Copy(CopySource Source, bool To, CopyTarget Target) : Statement
    {
        public Sequence<CopyOption>? Options { get; init; }
        // WITH options (before PostgreSQL version 9.0)
        public Sequence<CopyLegacyOption>? LegacyOptions { get; init; }
        // VALUES a vector of values to be copied
        public Sequence<string?>? Values { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("COPY");
            if (Source is CopySource.CopySourceQuery query)
            {
                writer.WriteSql($"({query})");
            }
            else if (Source is CopySource.Table table)
            {
                writer.WriteSql($" {table.TableName}");

                if (table.Columns.SafeAny())
                {
                    writer.Write($"({table.Columns.ToSqlDelimited()})");
                }
            }

            var direction = To ? "TO" : "FROM";
            writer.WriteSql($" {direction} {Target}");

            if (Options.SafeAny())
            {
                writer.WriteSql($" ({Options})");
            }

            if (LegacyOptions.SafeAny())
            {
                writer.Write($" {LegacyOptions.ToSqlDelimited(Symbols.Space)}");
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
    /// COPPY INTO statement
    /// See https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
    /// Copy Into syntax available for Snowflake is different from the one implemented in
    /// Postgres. Although they share common prefix, it is reasonable to implement them
    /// in different enums. This can be refactored later once custom dialects
    /// are allowed to have custom Statements.
    /// </summary>
    /// <param name="Into">Into name</param>
    /// <param name="FromStage">From stage</param>
    /// <param name="FromStageAlias">From stage alias</param>
    /// <param name="StageParams">Stage params</param>
    /// <param name="FromTransformations">From transformations</param>
    /// <param name="Files">Files</param>
    /// <param name="Pattern">Pattern</param>
    /// <param name="FileFormat">File format</param>
    /// <param name="CopyOptions">Copy options</param>
    /// <param name="ValidationMode">Validation mode</param>
    public record CopyIntoSnowflake(
        ObjectName Into,
        ObjectName FromStage,
        Ident? FromStageAlias = null,
        StageParams? StageParams = null,
        Sequence<StageLoadSelectItem>? FromTransformations = null,
        Sequence<string>? Files = null,
        string? Pattern = null,
        Sequence<DataLoadingOption>? FileFormat = null,
        Sequence<DataLoadingOption>? CopyOptions = null,
        string? ValidationMode = null) : Statement
    {
        public StageParams StageParams = StageParams ?? new();

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"COPY INTO {Into}");

            if (FromTransformations == null || FromTransformations.Count == 0)
            {
                writer.WriteSql($" FROM {FromStage}{StageParams}");

                if (FromStageAlias != null)
                {
                    writer.WriteSql($" AS {FromStageAlias}");
                }
            }
            else
            {
                writer.WriteSql($" FROM (SELECT {FromTransformations.ToSqlDelimited()} FROM {FromStage}{StageParams}");

                if (FromStageAlias != null)
                {
                    writer.WriteSql($" AS {FromStageAlias}");
                }

                writer.Write(")");
            }

            if (Files != null && Files.Count != 0)
            {
                writer.WriteSql($" FILES = ('");
                writer.Write(string.Join("', '", Files));
                writer.Write("')");
            }

            if (Pattern != null)
            {
                writer.WriteSql($" PATTERN = '{Pattern}'");
            }

            if (FileFormat != null && FileFormat.Count != 0)
            {
                writer.WriteSql($" FILE_FORMAT=({FileFormat.ToSqlDelimited(Symbols.Space)})");
            }

            if (CopyOptions != null && CopyOptions.Count != 0)
            {
                writer.WriteSql($" COPY_OPTIONS=({CopyOptions.ToSqlDelimited(Symbols.Space)})");
            }

            if (ValidationMode != null)
            {
                writer.WriteSql($" VALIDATION_MODE = {ValidationMode}");
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
    /// Create extension statement
    /// </summary>
    /// <param name="Name">Name</param>
    /// <param name="IfNotExists">True if not exists</param>
    /// <param name="Cascade">Cascade</param>
    /// <param name="Schema">Schema</param>
    /// <param name="Version">Version</param>
    public record CreateExtension(Ident Name, bool IfNotExists, bool Cascade, Ident? Schema, Ident? Version) : Statement, IIfNotExists
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var ifNotExists = IfNotExists ? $"{IIfNotExists.IfNotExistsPhrase} " : null;

            writer.WriteSql($"CREATE EXTENSION {ifNotExists}{Name}");

            if (Cascade || Schema != null || Version != null)
            {
                writer.Write(" WITH");

                if (Schema != null)
                {
                    writer.WriteSql($" SCHEMA {Schema}");
                }

                if (Version != null)
                {
                    writer.WriteSql($" VERSION {Version}");
                }

                if (Cascade)
                {
                    writer.Write(" CASCADE");
                }
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
    public record CreateFunction([Visit(0)] ObjectName Name) : Statement, IIfNotExists
    {
        public bool OrReplace { get; init; }
        public bool Temporary { get; init; }
        public bool IfNotExists { get; init; }
        [Visit(2)] public Sequence<OperateFunctionArg>? Args { get; init; }
        public DataType? ReturnType { get; init; }
        public CreateFunctionBody FunctionBody { get; init; }
        public FunctionBehavior? Behavior { get; init; }
        public FunctionCalledOnNull? CalledOnNull { get; init; }
        public FunctionParallel? Parallel { get; init; }
        public CreateFunctionUsing? Using { get; init; }
        public Ident? Language { get; init; }
        public FunctionDeterminismSpecifier? DeterminismSpecifier { get; init; }
        public Sequence<SqlOption>? Options { get; init; }
        public ObjectName? RemoteConnection { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var ifNot = IfNotExists ? $"{AsIne.IfNotExistsText} " : null;
            var or = OrReplace ? "OR REPLACE " : null;
            var temp = Temporary ? "TEMPORARY " : null;

            writer.WriteSql($"CREATE {or}{temp}FUNCTION {ifNot}{Name}");

            if (Args.SafeAny())
            {
                writer.WriteSql($"({Args})");
            }

            if (ReturnType != null)
            {
                writer.WriteSql($" RETURNS {ReturnType}");
            }

            if (DeterminismSpecifier != null)
            {
                writer.WriteSql($" {DeterminismSpecifier}");
            }
            if (Language != null)
            {
                writer.WriteSql($" LANGUAGE {Language}");
            }
            if (Behavior != null)
            {
                writer.WriteSql($" {Behavior}");
            }
            if (CalledOnNull != null)
            {
                writer.WriteSql($" {CalledOnNull}");
            }
            if (Parallel != null)
            {
                writer.WriteSql($" {Parallel}");
            }
            if (RemoteConnection != null)
            {
                writer.WriteSql($" REMOTE WITH CONNECTION {RemoteConnection}");
            }

            switch (FunctionBody)
            {
                case CreateFunctionBody.AsBeforeOptions b:
                    writer.WriteSql($" AS {b}");
                    break;

                case CreateFunctionBody.Return r:
                    writer.WriteSql($" RETURN {r}");
                    break;
            }

            if (Using != null)
            {
                writer.WriteSql($" {Using}");
            }
            if (Options != null)
            {
                writer.WriteSql($" OPTIONS({Options.ToSqlDelimited()})");
            }
            if (FunctionBody is CreateFunctionBody.AsAfterOptions f)
            {
                writer.WriteSql($" AS {f}");
            }
            //writer.WriteSql($"{Parameters}");
        }
    }
    /// <summary>
    /// Create Index statement
    /// </summary>
    public record CreateIndex(Ast.CreateIndex Element) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Element}");
        }
    }

    public record CreatePolicy(Ident Name, ObjectName TableName) : Statement
    {
        public CreatePolicyType? PolicyType { get; init; }
        public CreatePolicyCommand? Command { get; init; }
        public Sequence<Owner>? To { get; init; }
        public Expression? Using { get; init; }
        public Expression? WithCheck { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"CREATE POLICY {Name} ON {TableName}");

            switch (PolicyType)
            {
                case CreatePolicyType.Permissive:
                    writer.Write(" AS PERMISSIVE");
                    break;
                case CreatePolicyType.Restrictive:
                    writer.Write(" AS RESTRICTIVE");
                    break;
            }

            switch (Command)
            {
                case CreatePolicyCommand.All:
                    writer.Write(" FOR PERMISSIVE");
                    break;
                case CreatePolicyCommand.Select:
                    writer.Write(" FOR SELECT");
                    break;
                case CreatePolicyCommand.Insert:
                    writer.Write(" FOR INSERT");
                    break;
                case CreatePolicyCommand.Update:
                    writer.Write(" FOR UPDATE");
                    break;
                case CreatePolicyCommand.Delete:
                    writer.Write(" FOR DELETE");
                    break;
            }

            if (To != null)
            {
                writer.WriteSql($" TO {To.ToSqlDelimited()}");
            }

            if (Using != null)
            {
                writer.WriteSql($" USING ({Using})");
            }

            if (WithCheck != null)
            {
                writer.WriteSql($" WITH CHECK ({WithCheck})");
            }
        }
    }
    /// <summary>
    /// MsSql Create Procedure statement
    /// </summary>
    /// <param name="OrAlter">Or alter flag</param>
    /// <param name="Name">Name</param>
    /// <param name="ProcedureParams">Procedure params</param>
    /// <param name="Body">Body statements</param>
    public record CreateProcedure(bool OrAlter, ObjectName Name, Sequence<ProcedureParam>? ProcedureParams, Sequence<Statement>? Body) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var orAlter = OrAlter ? "OR ALTER " : string.Empty;

            writer.WriteSql($"CREATE {orAlter}PROCEDURE {Name}");

            if (ProcedureParams.SafeAny())
            {
                writer.Write($" ({ProcedureParams.ToSqlDelimited()})");
            }

            writer.WriteSql($" AS BEGIN {Body} END");
        }
    }
    /// <summary>
    /// DuckDB Create Macro statement
    /// </summary>
    /// <param name="OrReplace">Or replace flag</param>
    /// <param name="Temporary">Temporary flag</param>
    /// <param name="Name">Name</param>
    /// <param name="Args">Macro args</param>
    /// <param name="Definition">Macro definition</param>
    public record CreateMacro(bool OrReplace, bool Temporary, ObjectName Name, Sequence<MacroArg>? Args, MacroDefinition Definition) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var orReplace = OrReplace ? "OR REPLACE " : string.Empty;
            var temp = Temporary ? "TEMPORARY " : string.Empty;
            writer.WriteSql($"CREATE {orReplace}{temp}Macro {Name}");

            if (Args.SafeAny())
            {
                writer.Write($"({Args.ToSqlDelimited()})");
            }

            if (Definition is MacroDefinition.MacroExpression e)
            {
                writer.WriteSql($" AS {e}");
            }
            else if (Definition is MacroDefinition.MacroTable t)
            {
                writer.WriteSql($" AS TABLE {t}");
            }
        }
    }
    public record CreateSecret(
        bool OrReplace,
        bool? Temporary,
        bool IfNotExists,
        Ident? Name,
        Ident? StorageIdentifier,
        Ident SecretType,
        Sequence<SecretOption> Options) : Statement, IIfNotExists
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var orReplace = OrReplace ? "OR REPLACE " : null;
            writer.WriteSql($"CREATE {orReplace}");

            if (Temporary != null)
            {
                var temp = Temporary.Value ? "TEMPORARY " : "PERSISTENT ";
                writer.Write(temp);
            }

            var ifNotExists = IfNotExists ? $" {IIfNotExists.IfNotExistsPhrase}" : null;
            writer.WriteSql($"SECRET{ifNotExists} ");

            if (Name != null)
            {
                writer.WriteSql($"{Name} ");
            }

            if (StorageIdentifier != null)
            {
                writer.WriteSql($"IN {StorageIdentifier} ");
            }

            writer.WriteSql($"( TYPE {SecretType}");
            if (Options.SafeAny())
            {
                writer.Write($", {Options.ToSqlDelimited()}");
            }
            writer.Write(" )");
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
        // ReSharper disable once MemberHidesStaticFromOuterClass
        public new string? Comment { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var orReplace = OrReplace ? "OR REPLACE " : null;
            var temp = Temporary ? "TEMPORARY " : null;
            var ifNot = IfNotExists ? $"{AsIne.IfNotExistsText} " : null;

            writer.WriteSql($"CREATE {orReplace}{temp}STAGE {ifNot}{Name}{StageParams}");

            if (DirectoryTableParams.SafeAny())
            {
                writer.WriteSql($" DIRECTORY=({DirectoryTableParams.ToSqlDelimited(Symbols.Space)})");
            }
            if (FileFormat.SafeAny())
            {
                writer.WriteSql($" FILE_FORMAT=({FileFormat.ToSqlDelimited(Symbols.Space)})");
            }
            if (CopyOptions.SafeAny())
            {
                writer.WriteSql($" COPY_OPTIONS=({CopyOptions.ToSqlDelimited(Symbols.Space)})");
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
    /// <param name="Element">Table columns</param>
    public record CreateTable(Ast.CreateTable Element) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Element}");
        }
    }
    /// <summary>
    /// CREATE TRIGGER
    /// </summary>
    public record CreateTrigger(ObjectName Name) : Statement
    {
        public bool OrReplace { get; init; }
        public bool IsConstraint { get; init; }
        public TriggerPeriod Period { get; init; }
        public ObjectName TableName { get; init; }
        public Sequence<TriggerEvent> Events { get; init; }
        public ObjectName? ReferencedTableName { get; init; }
        public Sequence<TriggerReferencing> Referencing { get; init; }
        public TriggerObject TriggerObject { get; init; }
        public bool IncludeEach { get; init; }
        public Expression? Condition { get; init; }
        public TriggerExecBody ExecBody { get; init; }
        public ConstraintCharacteristics? Characteristics { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var orReplace = OrReplace ? "OR REPLACE " : string.Empty;
            var isConstraint = IsConstraint ? " CONSTRAINT " : string.Empty;
            writer.WriteSql($"CREATE {orReplace}{isConstraint}TRIGGER {Name} {Period}");

            if (Events.SafeAny())
            {
                writer.Write(" ");
                writer.WriteDelimited(Events, " OR ");
            }

            writer.WriteSql($" ON {TableName}");

            if (ReferencedTableName != null)
            {
                writer.WriteSql($" FROM {ReferencedTableName}");
            }

            if (Characteristics != null)
            {
                writer.WriteSql($" {Characteristics}");
            }

            if (Referencing.SafeAny())
            {
                writer.WriteSql($" REFERENCING {Referencing.ToSqlDelimited(" ")}");
            }

            if (IncludeEach)
            {
                writer.WriteSql($" FOR EACH {TriggerObject}");
            }
            else
            {
                writer.WriteSql($" FOR {TriggerObject}");
            }

            if (Condition != null)
            {
                writer.WriteSql($" WHEN {Condition}");
            }

            writer.WriteSql($" EXECUTE {ExecBody}");
        }
    }
    /// <summary>
    /// Create View statement
    /// </summary>
    /// <param name="Name">Object name</param>
    public record CreateView([property: Visit(0)] ObjectName Name, [property: Visit(1)] Select Query) : Statement,
        IIfNotExists
    {
        public bool OrReplace { get; init; }
        public bool Materialized { get; init; }
        public Sequence<ViewColumnDef>? Columns { get; init; }
        public required CreateTableOptions Options { get; init; }
        public Sequence<Ident>? ClusterBy { get; init; }
        public string? Comment { get; init; }
        public bool WithNoSchemaBinding { get; init; }
        public bool IfNotExists { get; init; }
        public bool Temporary { get; init; }
        public ObjectName? To { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var orReplace = OrReplace ? "OR REPLACE " : null;
            var materialized = Materialized ? "MATERIALIZED " : null;
            var temporary = Temporary ? "TEMPORARY " : null;
            var ifNotExists = IfNotExists ? $"{IIfNotExists.IfNotExistsPhrase} " : null;
            var to = To != null ? $" TO {To.ToSql()}" : null;

            writer.WriteSql($"CREATE {orReplace}{materialized}{temporary}VIEW {ifNotExists}{Name}{to}");

            if (Comment != null)
            {
                writer.WriteSql($" COMMENT = '{Comment.EscapeSingleQuoteString()}'");
            }

            if (Options is CreateTableOptions.With)
            {
                writer.WriteSql($" {Options}");
            }

            if (Columns.SafeAny())
            {
                writer.WriteSql($" ({Columns!.ToSqlDelimited()})");
            }

            if (ClusterBy.SafeAny())
            {
                writer.WriteSql($" CLUSTER BY ({ClusterBy!.ToSqlDelimited()})");
            }

            if (Options is CreateTableOptions.Options)
            {
                writer.WriteSql($"{Options}");
            }

            writer.Write(" AS ");
            Query.ToSql(writer);

            if (WithNoSchemaBinding)
            {
                writer.Write(" WITH NO SCHEMA BINDING");
            }
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
    public record CreateSequence([property: Visit(0)] ObjectName Name) : Statement, IIfNotExists
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
    /// CREATE TYPE
    /// </summary>
    /// <param name="Name">Name</param>
    /// <param name="Representation">Representation</param>
    public record CreateType(ObjectName Name, UserDefinedTypeRepresentation Representation) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"CREATE TYPE {Name} AS {Representation}");
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
    /// <param name="Statements">Declare statements</param>
    public record Declare(Sequence<Ast.Declare> Statements) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DECLARE {Statements.ToSqlDelimited("; ")}");
        }
    }
    /// <summary>
    /// Delete statement
    /// </summary>
    /// <param name="DeleteOperation">Delete operation</param>
    public record Delete(DeleteOperation DeleteOperation) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DELETE ");

            if (DeleteOperation.Tables is { Count: > 0 })
            {
                writer.WriteDelimited(DeleteOperation.Tables, Constants.SpacedComma);
            }

            writer.WriteSql($"{DeleteOperation.From}");

            if (DeleteOperation.Using != null)
            {
                writer.WriteSql($" USING {DeleteOperation.Using}");
            }

            if (DeleteOperation.Selection != null)
            {
                writer.WriteSql($" WHERE {DeleteOperation.Selection}");
            }

            if (DeleteOperation.Returning != null)
            {
                writer.WriteSql($" RETURNING {DeleteOperation.Returning}");
            }

            if (DeleteOperation.OrderBy.SafeAny())
            {
                writer.Write($" ORDER BY {DeleteOperation.OrderBy.ToSqlDelimited()}");
            }

            if (DeleteOperation.Limit != null)
            {
                writer.WriteSql($" LIMIT {DeleteOperation.Limit}");
            }
        }
    }

    public record DetachDuckDbDatabase(
        bool IfExists,
        bool Database,
        Ident DatabaseAlias) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var ifNot = IfExists ? $" {IIfNotExists.IfExistsPhrase}" : null;
            var keyword = Database ? " DATABASE" : "";

            writer.WriteSql($"DETACH{keyword}{ifNot} {DatabaseAlias}");
        }
    }

    public record DropPolicy(bool IfExists, Ident Name, ObjectName TableName, ReferentialAction? Option = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DROP POLICY");

            if (IfExists)
            {
                writer.Write(" IF EXISTS");
            }

            writer.WriteSql($" {Name} ON {TableName}");

            if (Option != null && Option != ReferentialAction.None)
            {
                writer.WriteSql($" {Option}");
            }
        }
    }

    public record DropSecret(
        bool IfExists,
        bool? Temporary,
        Ident Name,
        Ident? StorageSpecifier) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DROP ");
            if (Temporary != null)
            {
                var temp = Temporary.Value ? "TEMPORARY " : "PERSISTENT ";
                writer.Write(temp);
            }

            var ifNotExists = IfExists ? $" {IIfNotExists.IfExistsPhrase}" : null;
            writer.WriteSql($"SECRET{ifNotExists} {Name}");

            if (StorageSpecifier != null)
            {
                writer.WriteSql($" FROM {StorageSpecifier}");
            }
        }
    }

    public record DropTrigger(bool IfExists, ObjectName TriggerName, ObjectName TableName, ReferentialAction Option) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DROP TRIGGER");

            if (IfExists)
            {
                writer.Write(" IF EXISTS");
            }

            writer.WriteSql($" {TriggerName} ON {TableName}");
        }
    }
    /// <summary>
    /// EXECUTE name [ ( parameter [, ...] ) ] [USING <expr>]
    /// </summary>
    public record Execute(Ident Name, Sequence<Expression>? Parameters, Sequence<Expression>? Using) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"EXECUTE {Name}");

            if (Parameters.SafeAny())
            {
                writer.Write($"({Parameters.ToSqlDelimited()})");
            }

            if (Using.SafeAny())
            {
                writer.Write($" USING {Using.ToSqlDelimited()}");
            }
        }
    }
    /// <summary>
    /// Install statement
    /// </summary>
    public record Install(Ident ExtensionName) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"INSTALL {ExtensionName}");
        }
    }
    /// <summary>
    /// Load statement
    /// </summary>
    public record Load(Ident ExtensionName) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"LOAD {ExtensionName}");
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
        /// Hive allows you to specify whether the table's stored data will be
        /// deleted along with the dropped table
        public bool Purge { get; init; }
        /// <summary>
        /// MySQL-specific "TEMPORARY" keyword
        /// </summary>
        public bool Temporary { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var ifExists = IfExists ? " IF EXISTS" : null;
            var cascade = Cascade ? " CASCADE" : null;
            var restrict = Restrict ? " RESTRICT" : null;
            var purge = Purge ? " PURGE" : null;
            var temporary = Temporary ? "TEMPORARY " : null;

            writer.WriteSql($"DROP {temporary}{ObjectType}{ifExists} {Names}{cascade}{restrict}{purge}");
        }
    }
    /// <summary>
    /// DROP Function statement
    /// </summary>
    /// <param name="IfExists">True if exists</param>
    /// <param name="FuncDesc">Drop function descriptions</param>
    /// <param name="Option">Referential actions</param>
    public record DropFunction(bool IfExists, Sequence<FunctionDesc> FuncDesc, ReferentialAction Option) : Statement
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
    public record FunctionDesc(ObjectName Name, Sequence<OperateFunctionArg>? Args = null) : Statement
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

    public record DropProcedure(bool IfExists, Sequence<FunctionDesc> ProcDescription, ReferentialAction? Option) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var ifEx = IfExists ? " IF EXISTS" : null;
            writer.WriteSql($"DROP PROCEDURE{ifEx} {ProcDescription.ToSqlDelimited()}");

            if (Option != null && Option != ReferentialAction.None)
            {
                writer.Write($" {Option}");
            }
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
        public DescribeAlias DescribeAlias { get; init; }

        // Display additional information regarding the plan.
        public bool Verbose { get; init; }

        /// Optional output format of explain
        public AnalyzeFormat Format { get; init; }

        public Sequence<UtilityOption>? Options { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{DescribeAlias} ");

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

            if (Options != null)
            {
                writer.WriteSql($"({Options.ToSqlDelimited()}) ");
            }

            Statement.ToSql(writer);
        }
    }
    /// <summary>
    /// EXPLAIN TABLE
    /// Note: this is a MySQL-specific statement. <see href="https://dev.mysql.com/doc/refman/8.0/en/explain.html"/>
    /// </summary>
    /// <param name="DescribeAlias">Query used the DESCRIBE alias for explain</param>
    /// <param name="Name">Table name</param>
    /// <param name="HiveFormat">Hive format</param>
    /// <param name="HasTableKeyword">True if statement has Table keyword; otherwise false</param>
    public record ExplainTable(
        DescribeAlias DescribeAlias,
        ObjectName Name,
        HiveDescribeFormat? HiveFormat,
        bool HasTableKeyword = false) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            // writer.Write(DescribeAlias ? "DESCRIBE " : "EXPLAIN ");
            writer.WriteSql($"{DescribeAlias} ");

            if (HiveFormat != null)
            {
                writer.WriteSql($"{HiveFormat} ");
            }

            if (HasTableKeyword)
            {
                writer.Write("TABLE ");
            }

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
    /// Flush statement
    /// </summary>
    /// <param name="ObjectType">Object type</param>
    /// <param name="Location">Location</param>
    /// <param name="Channel">Channel</param>
    /// <param name="ReadLock">Read lock</param>
    /// <param name="Export">Export</param>
    /// <param name="Tables">Tables</param>
    public record Flush(FlushType ObjectType, FlushLocation? Location, string? Channel, bool ReadLock, bool Export, Sequence<ObjectName>? Tables) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FLUSH");
            if (Location != null)
            {
                writer.WriteSql($" {Location}");
            }

            writer.WriteSql($" {ObjectType}");
            if (Channel != null)
            {
                writer.Write($" FOR CHANNEL {Channel}");
            }

            if (Tables.SafeAny())
            {
                writer.Write($" {Tables.ToSqlDelimited()}");
            }

            var export = Export ? " FOR EXPORT" : null;
            var read = ReadLock ? " WITH READ LOCK" : null;

            writer.WriteSql($"{read}{export}");
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
    /// <param name="InsertOperation">Insert operation</param>
    public record Insert(InsertOperation InsertOperation) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var tableName = InsertOperation.Alias != null ? $"{InsertOperation.Name.ToSql()} as {InsertOperation.Alias.ToSql()}" : $"{InsertOperation.Name.ToSql()}";

            if (InsertOperation.Or != SqliteOnConflict.None)
            {
                writer.WriteSql($"INSERT OR {InsertOperation.Or} INTO {tableName} ");
            }
            else
            {
                writer.Write(InsertOperation.ReplaceInto ? "REPLACE" : "INSERT");

                if (InsertOperation.Priority != MySqlInsertPriority.None)
                {
                    writer.WriteSql($" {InsertOperation.Priority}");
                }

                var over = InsertOperation.Overwrite ? " OVERWRITE" : null;
                var into = InsertOperation.Into ? " INTO" : null;
                var table = InsertOperation.Table ? " TABLE" : null;
                var ignore = InsertOperation.Ignore ? " IGNORE" : null;
                writer.Write($"{ignore}{over}{into}{table} {tableName} ");
            }

            if (InsertOperation.Columns.SafeAny())
            {
                writer.WriteSql($"({InsertOperation.Columns}) ");
            }

            if (InsertOperation.Partitioned.SafeAny())
            {
                writer.WriteSql($"PARTITION ({InsertOperation.Partitioned}) ");
            }

            if (InsertOperation.AfterColumns.SafeAny())
            {
                writer.WriteSql($"({InsertOperation.AfterColumns}) ");
            }

            if (InsertOperation.Source != null)
            {
                InsertOperation.Source.ToSql(writer);
            }
            else if (!InsertOperation.Columns.SafeAny())
            {
                writer.Write("DEFAULT VALUES");
            }

            if (InsertOperation.InsertAlias != null)
            {
                writer.WriteSql($" AS {InsertOperation.InsertAlias.RowAlias}");

                if (InsertOperation.InsertAlias.ColumnAliases.SafeAny())
                {
                    writer.Write($" ({InsertOperation.InsertAlias.ColumnAliases.ToSqlDelimited()})");
                }
            }

            InsertOperation.On?.ToSql(writer);

            if (InsertOperation.Returning.SafeAny())
            {
                writer.WriteSql($" RETURNING {InsertOperation.Returning}");
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
    /// MySql `LOCK TABLES table_name  [READ [LOCAL] | [LOW_PRIORITY] WRITE]`
    /// </summary>
    /// <param name="Tables"></param>
    public record LockTables(Sequence<LockTable> Tables) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"LOCK TABLES {Tables.ToSqlDelimited()}");
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
            writer.WriteSql($"MERGE{into} {Table} USING {Source} ON {On} {Clauses.ToSqlDelimited(Symbols.Space)}");
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
    /// OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE [BY expression]]
    /// </summary>
    public record OptimizeTable(
        ObjectName Name,
        Ident? OnCluster = null,
        Partition? Partition = null,
        bool IncludeFinal = false,
        Deduplicate? Deduplicate = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"OPTIMIZE TABLE {Name}");

            if (OnCluster != null)
            {
                writer.WriteSql($" ON CLUSTER {OnCluster}");
            }

            if (Partition != null)
            {
                writer.WriteSql($" {Partition}");
            }

            if (IncludeFinal)
            {
                writer.Write(" FINAL");
            }

            if (Deduplicate != null)
            {
                writer.WriteSql($" {Deduplicate}");
            }
        }
    }
    public record Pragma(ObjectName Name, Value? Value, bool IsEqual) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"PRAGMA {Name}");
            if (Value != null)
            {
                if (IsEqual)
                {
                    writer.WriteSql($" = {Value}");
                }
                else
                {
                    writer.WriteSql($"({Value})");

                }
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

    public record ReleaseSavepoint(Ident Name) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RELEASE SAVEPOINT {Name}");
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
    public record Rollback(bool Chain, Ident? SavePoint = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var chain = Chain ? " AND CHAIN" : null;
            writer.Write($"ROLLBACK{chain}");

            if (SavePoint != null)
            {
                writer.WriteSql($" TO SAVEPOINT {SavePoint}");
            }
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
    /// <param name="Variables">Variable names</param>
    /// <param name="Value">Value</param>
    public record SetVariable(bool Local, bool HiveVar, OneOrManyWithParens<ObjectName> Variables, Sequence<Expression>? Value = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SET ");

            if (Local)
            {
                writer.Write("LOCAL ");
            }

            var parenthesized = Variables is OneOrManyWithParens<ObjectName>.Many;

            var hiveVar = HiveVar ? "HIVEVAR:" : null;

            var leftParen = parenthesized ? "(" : string.Empty;
            var rightParen = parenthesized ? ")" : string.Empty;

            writer.WriteSql($"{hiveVar}{Variables} = {leftParen}{Value}{rightParen}");
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
    /// SHOW [GLOBAL | SESSION] STATUS [LIKE 'pattern' | WHERE expr]
    /// </summary>
    public record ShowStatus(ShowStatementFilter? Filter, bool Session, bool Global) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SHOW");

            if (Global)
            {
                writer.Write(" GLOBAL");
            }

            if (Session)
            {
                writer.Write(" SESSION");
            }
            writer.Write(" STATUS");

            if (Filter != null)
            {
                writer.WriteSql($" {Filter}");
            }
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
                writer.WriteSql($" {Variable.ToSqlDelimited(Symbols.Space)}");
            }
        }
    }
    /// <summary>
    /// SHOW VARIABLES
    /// </summary>
    /// <param name="Filter">Show statement filter</param>
    public record ShowVariables(ShowStatementFilter? Filter, bool Global, bool Session) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SHOW");

            if (Global)
            {
                writer.Write(" GLOBAL");
            }
            if (Session)
            {
                writer.Write(" SESSION");
            }

            writer.Write(" VARIABLES");

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
    public record StartTransaction(Sequence<TransactionMode>? Modes, bool Begin, TransactionModifier? Modifier = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Begin)
            {
                if (Modifier != null)
                {
                    writer.WriteSql($"BEGIN {Modifier} TRANSACTION");
                }
                else
                {
                    writer.Write("BEGIN TRANSACTION");
                }
            }
            else
            {
                writer.Write("START TRANSACTION");
            }


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
    public record Truncate(
        Sequence<TruncateTableTarget> Names,
        bool Table,
        bool Only,
        Sequence<Expression>? Partitions = null,
        TruncateIdentityOption? Identity = null,
        TruncateCascadeOption? Cascade = null,
        Ident? OnCluster = null) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var table = Table ? "TABLE " : string.Empty;
            var only = Only ? "ONLY " : string.Empty;

            writer.WriteSql($"TRUNCATE {table}{only}{Names.ToSqlDelimited()}");

            if (Identity != null)
            {
                switch (Identity)
                {
                    case TruncateIdentityOption.Restart:
                        writer.Write(" RESTART IDENTITY");
                        break;

                    case TruncateIdentityOption.Continue:
                        writer.Write(" CONTINUE IDENTITY");
                        break;
                }
            }

            if (Cascade != null)
            {
                switch (Cascade)
                {
                    case TruncateCascadeOption.Cascade:
                        writer.Write(" CASCADE");
                        break;

                    case TruncateCascadeOption.Restrict:
                        writer.Write(" RESTRICT");
                        break;
                }
            }

            if (Partitions.SafeAny())
            {
                writer.WriteSql($" PARTITION ({Partitions})");
            }

            if (OnCluster != null)
            {
                writer.WriteSql($" ON CLUSTER {OnCluster}");
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
    /// MySql `Unlock Tables`
    /// </summary>
    public record UnlockTables : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("UNLOCK TABLES");
        }
    }

    public record Unload(Query Query, Ident To, Sequence<SqlOption> With) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"UNLOAD({Query}) TO {To}");

            if (With.SafeAny())
            {
                writer.Write($" WITH ({With.ToSqlDelimited()})");
            }
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
    public record Use(Ast.Use Name) : Statement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name}");
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