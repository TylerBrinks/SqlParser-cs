namespace SqlParser.Ast;

/// <summary>
/// Alter table operations
/// </summary>
public abstract record AlterTableOperation : IWriteSql
{
    /// <summary>
    /// Add table constraint operation
    /// <example>
    /// <c>
    /// ADD table_constraint
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="TableConstraint">Table Constraint</param>
    public record AddConstraint(TableConstraint TableConstraint) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ADD {TableConstraint}");
        }
    }
    /// <summary>
    ///  Add column operation
    /// <example>
    /// <c>
    /// ADD [COLUMN] [IF NOT EXISTS] column_def
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="ColumnKeyword">Contains column keyword</param>
    /// <param name="IfNotExists">Contains If Not Exists</param>
    /// <param name="ColumnDef">Column Definition</param>
    public record AddColumn(
        bool ColumnKeyword,
        bool IfNotExists,
        ColumnDef ColumnDef,
        MySqlColumnPosition? ColumnPosition = null)
        : AlterTableOperation, IIfNotExists
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("ADD");

            if (ColumnKeyword)
            {
                writer.Write(" COLUMN");
            }

            if (IfNotExists)
            {
                writer.Write($" {IIfNotExists.IfNotExistsPhrase}");
            }

            writer.WriteSql($" {ColumnDef}");

            if (ColumnPosition != null)
            {
                writer.WriteSql($" {ColumnPosition}");
            }
        }
    }
    /// <summary>
    ///  Add partitions table operation
    /// <example>
    /// <c>
    /// ADD PARTITION
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="IfNotExists"></param>
    /// <param name="NewPartitions"></param>
    public record AddPartitions(bool IfNotExists, Sequence<Partition> NewPartitions) : AlterTableOperation, IIfNotExists
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var ifNot = IfNotExists ? $" {IIfNotExists.IfNotExistsPhrase}" : null;

            writer.WriteSql($"ADD{ifNot} {NewPartitions.ToSqlDelimited(Symbols.Space)}");
        }
    }
    /// <summary>
    /// ADD PROJECTION [IF NOT EXISTS] name ( SELECT [COLUMN LIST EXPR] [GROUP BY] [ORDER BY])
    /// </summary>
    /// <param name="IfNotExists">If not exists</param>
    /// <param name="Name">Projection name</param>
    /// <param name="Select">Projection select</param>
    public record AddProjection(bool IfNotExists, Ident Name, ProjectionSelect Select) : AlterTableOperation, IIfNotExists
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ADD PROJECTION");

            if (IfNotExists)
            {
                writer.Write($" {IIfNotExists.IfNotExistsPhrase}");
            }

            writer.WriteSql($" {Name} ({Select})");
        }
    }
    /// <summary>
    /// Alter column table operation
    /// <example>
    /// <c>
    /// ALTER [ COLUMN ]
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="ColumnName">Column Name</param>
    /// <param name="Operation">Alter column operation</param>
    public record AlterColumn(Ident ColumnName, AlterColumnOperation Operation) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ALTER COLUMN {ColumnName} {Operation}");
        }
    }
    /// <summary>
    /// `ATTACH PART|PARTITION partition_expr`
    /// </summary>
    /// <param name="Partition">Partition</param>
    public record AttachPartition(Partition Partition) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ATTACH {Partition}");
        }
    }
    /// <summary>
    /// CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
    /// </summary>
    /// <param name="IfExists">Materialize if exists</param>
    /// <param name="Name">Name</param>
    /// <param name="Partition">Partition</param>
    public record ClearProjection(bool IfExists, Ident Name, Ident? Partition) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("CLEAR PROJECTION");

            if (IfExists)
            {
                writer.Write(" IF EXISTS");
            }

            writer.Write($" {Name}");

            if (Partition != null)
            {
                writer.Write($" IN PARTITION {Partition}");
            }
        }
    }
    /// <summary>
    /// `DETACH PART|PARTITION partition_expr`
    /// </summary>
    /// <param name="Partition">Partition</param>
    public record DetachPartition(Partition Partition) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DETACH {Partition}");
        }
    }
    /// <summary>
    /// DISABLE ROW LEVEL SECURITY
    /// Note: this is a PostgreSQL-specific operation.
    /// </summary>
    public record DisableRowLevelSecurity : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DISABLE ROW LEVEL SECURITY");
        }
    }
    /// <summary>
    /// DISABLE RULE rewrite_rule_name
    /// Note: this is a PostgreSQL-specific operation.
    /// </summary>
    public record DisableRule(Ident Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DISABLE RULE {Name}");
        }
    }
    /// <summary>
    /// DISABLE TRIGGER [ trigger_name | ALL | USER ]
    /// Note: this is a PostgreSQL-specific operation.
    /// </summary>
    public record DisableTrigger(Ident Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DISABLE TRIGGER {Name}");
        }
    }
    /// <summary>
    /// Drop constraint table operation
    /// <example>
    /// <c>
    /// DROP CONSTRAINT [ IF EXISTS ] name
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name">Name identifier</param>
    /// <param name="IfExists">Drop If Exists</param>
    /// <param name="Cascade">Cascade</param>
    public record DropConstraint(Ident Name, bool IfExists, bool Cascade) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {

            var ifExists = IfExists ? "IF EXISTS " : null;
            var cascade = Cascade ? " CASCADE" : null;

            writer.WriteSql($"DROP CONSTRAINT {ifExists}{Name}{cascade}");
        }
    }
    /// <summary>
    /// Drop column table operation
    /// <example>
    /// <c>
    ///  DROP [ COLUMN ] [ IF EXISTS ] column_name [ CASCADE ]
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name"></param>
    /// <param name="IfExists"></param>
    /// <param name="Cascade"></param>
    public record DropColumn(Ident Name, bool IfExists, bool Cascade) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {

            var ifExists = IfExists ? "IF EXISTS " : null;
            var cascade = Cascade ? " CASCADE" : null;

            writer.WriteSql($"DROP COLUMN {ifExists}{Name}{cascade}");
        }
    }
    /// <summary>
    /// Drop primary key table operation
    /// 
    /// Note: this is a MySQL-specific operation.
    /// <example>
    /// <c>
    /// DROP PRIMARY KEY
    /// </c>
    /// </example>
    /// </summary>
    /// <summary>
    /// Drop partitions table operation
    /// <example>
    /// <c>
    /// DROP PARTITION
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Partitions">Partitions</param>
    /// <param name="IfExists">Contains If Not Exists</param>
    public record DropPartitions(Sequence<Expression> Partitions, bool IfExists) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var ie = IfExists ? " IF EXISTS" : null;
            writer.WriteSql($"DROP{ie} PARTITION ({Partitions})");
        }
    }

    public record DropPrimaryKey : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DROP PRIMARY KEY");
        }
    }
    /// <summary>
    /// DROP PROJECTION [IF EXISTS] name
    /// </summary>
    /// <param name="IfExists">Drop If Exists</param>
    /// <param name="Name">Name identifier</param>
    public record DropProjection(bool IfExists, Ident Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DROP PROJECTION");

            if (IfExists)
            {
                writer.Write(" IF EXISTS");

            }

            writer.Write($" {Name}");
        }
    }
    /// ENABLE ALWAYS RULE rewrite_rule_name
    ///
    /// Note: this is a PostgreSQL-specific operation.
    public record EnableAlwaysRule(Ident Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ENABLE ALWAYS RULE {Name}");
        }
    }
    /// ENABLE ALWAYS TRIGGER trigger_name
    ///
    /// Note: this is a PostgreSQL-specific operation.
    public record EnableAlwaysTrigger(Ident Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ENABLE ALWAYS TRIGGER {Name}");
        }
    }
    /// ENABLE REPLICA RULE rewrite_rule_name
    ///
    /// Note: this is a PostgreSQL-specific operation.
    public record EnableReplicaRule(Ident Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ENABLE REPLICA RULE {Name}");
        }
    }
    /// ENABLE REPLICA TRIGGER trigger_name
    ///
    /// Note: this is a PostgreSQL-specific operation.
    public record EnableReplicaTrigger(Ident Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ENABLE REPLICA TRIGGER {Name}");
        }
    }
    /// ENABLE ROW LEVEL SECURITY
    ///
    /// Note: this is a PostgreSQL-specific operation.
    public record EnableRowLevelSecurity : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ENABLE ROW LEVEL SECURITY");
        }
    }
    /// ENABLE RULE rewrite_rule_name
    ///
    /// Note: this is a PostgreSQL-specific operation.
    public record EnableRule(Ident Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ENABLE RULE {Name}");
        }
    }
    /// ENABLE TRIGGER [ trigger_name | ALL | USER ]
    ///
    /// Note: this is a PostgreSQL-specific operation.
    public record EnableTrigger(Ident Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ENABLE TRIGGER {Name}");
        }
    }
    /// <summary>
    /// FREEZE PARTITION partition_expr
    /// </summary>
    public record FreezePartition(Partition Partition, Ident? WithName = null) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"FREEZE {Partition}");

            if (WithName != null)
            {
                writer.WriteSql($" WITH NAME {WithName}");
            }
        }
    }
    /// <summary>
    /// CHANGE [ COLUMN ] col_name data_type [ options ]
    /// </summary>
    public record ModifyColumn(
        Ident ColumnName,
        DataType DataType,
        Sequence<ColumnOption> Options,
        MySqlColumnPosition? ColumnPosition)
        : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"MODIFY COLUMN {ColumnName} {DataType}");

            if (Options.SafeAny())
            {
                writer.Write($" {Options.ToSqlDelimited(Symbols.Space)}");
            }

            if (ColumnPosition != null)
            {
                writer.WriteSql($" {ColumnPosition}");
            }
        }
    }
    /// <summary>
    /// Rename partitions table operation
    /// <example>
    /// <c>
    /// RENAME TO PARTITION (partition=val)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="OldPartitions">Old partitions</param>
    /// <param name="NewPartitions">New partitions</param>
    public record RenamePartitions(Sequence<Expression> OldPartitions, Sequence<Expression> NewPartitions) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"PARTITION ({OldPartitions}) RENAME TO PARTITION ({NewPartitions})");
        }
    }
    /// <summary>
    /// Rename column table operation
    /// <example>
    /// <c>
    ///  RENAME [ COLUMN ] old_column_name TO new_column_name
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="OldColumnName">Old column names</param>
    /// <param name="NewColumnName">New column names</param>
    public record RenameColumn(Ident OldColumnName, Ident NewColumnName) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME COLUMN {OldColumnName} TO {NewColumnName}");
        }
    }
    /// <summary>
    /// Rename table table operation
    /// <example>
    /// <c>
    /// RENAME TO table_name
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name">Table name</param>
    public record RenameTable(ObjectName Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME TO {Name}");
        }
    }
    /// <summary>
    /// Change column  table operation
    /// <example>
    /// <c>
    /// CHANGE [ COLUMN ] old_name new_name data_type [ options ]
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="OldName">Old name</param>
    /// <param name="NewName">New name</param>
    /// <param name="DataType">Data type</param>
    /// <param name="Options">Rename options</param>
    public record ChangeColumn(
        Ident OldName,
        Ident NewName,
        DataType DataType,
        Sequence<ColumnOption> Options,
        MySqlColumnPosition? ColumnPosition = null)
        : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"CHANGE COLUMN {OldName} {NewName} {DataType}");
            if (Options.Any())
            {
                writer.WriteSql($" {Options.ToSqlDelimited(Symbols.Space)}");
            }

            if (ColumnPosition != null)
            {
                writer.WriteSql($" {ColumnPosition}");
            }
        }
    }
    /// <summary>
    /// MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
    /// </summary>
    /// <param name="IfExists">Materialize if exists</param>
    /// <param name="Name">Name</param>
    /// <param name="Partition">Partition</param>
    public record MaterializeProjection(bool IfExists, Ident Name, Ident? Partition) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("MATERIALIZE PROJECTION");

            if (IfExists)
            {
                writer.Write(" IF EXISTS");
            }

            writer.Write($" {Name}");

            if (Partition != null)
            {
                writer.Write($" IN PARTITION {Partition}");
            }
        }
    }
    /// <summary>
    /// Rename Constraint table operation
    ///
    ///  Note: this is a PostgreSQL-specific operation.
    /// <example>
    /// <c>
    /// RENAME CONSTRAINT old_constraint_name TO new_constraint_name
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="OldName"></param>
    /// <param name="NewName"></param>
    public record RenameConstraint(Ident OldName, Ident NewName) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME CONSTRAINT {OldName} TO {NewName}");
        }
    }
    /// <summary>
    /// 
    /// </summary>
    /// <param name="Name"></param>
    public record SwapWith(ObjectName Name) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SWAP WITH {Name}");
        }
    }

    public record OwnerTo(Owner NewOwner) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"OWNER TO {NewOwner}");
        }
    }
    /// <summary>
    /// UNFREEZE PARTITION partition_expr
    /// </summary>
    public record UnfreezePartition(Partition Partition, Ident? WithName = null) : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"UNFREEZE {Partition}");

            if (WithName != null)
            {
                writer.WriteSql($" WITH NAME {WithName}");
            }
        }
    }
    public abstract void ToSql(SqlTextWriter writer);
}
