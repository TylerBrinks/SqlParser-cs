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
    public record AddConstraint(TableConstraint TableConstraint) : AlterTableOperation, IElement
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
    public record AddColumn(bool ColumnKeyword, bool IfNotExists, ColumnDef ColumnDef) : AlterTableOperation, IIfNotExists, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("ADD");

            if(ColumnKeyword) {
                writer.Write(" COLUMN");
            }

            if(IfNotExists)
            {
                writer.Write($" {IIfNotExists.IfNotExistsPhrase}");
            }

            writer.WriteSql($" {ColumnDef}");
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
    /// <param name="IfExists">Contains If Exists</param>
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
    public record DropPrimaryKey : AlterTableOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DROP PRIMARY KEY");
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
    public record RenamePartitions(Sequence<Expression> OldPartitions, Sequence<Expression> NewPartitions) : AlterTableOperation, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"PARTITION ({OldPartitions}) RENAME TO PARTITION ({NewPartitions})");
        }
    }
    /// <summary>
    /// Add partitions table operation
    /// <example>
    /// <c>
    /// ADD PARTITION
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="IfNotExists"></param>
    /// <param name="NewPartitions"></param>
    public record AddPartitions(bool IfNotExists, Sequence<Partition> NewPartitions) : AlterTableOperation, IIfNotExists, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var ifNot = IfNotExists ? $" {IIfNotExists.IfNotExistsPhrase}" : null;

            //writer.WriteSql($"ADD{ifNot} PARTITION ({NewPartitions})");
            writer.WriteSql($"ADD{ifNot} ");
            writer.WriteDelimited(NewPartitions, " ");
        }
    }
    /// <summary>
    /// Drop partitions table operation
    /// <example>
    /// <c>
    /// DROP PARTITION
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Partitions">Partitions sto drop</param>
    /// <param name="IfExists">Contains If Not Exists</param>
    public record DropPartitions(Sequence<Expression> Partitions, bool IfExists) : AlterTableOperation, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var ie = IfExists ? " IF EXISTS" : null;
            writer.WriteSql($"DROP{ie} PARTITION ({Partitions})");
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
    public record RenameTable(ObjectName Name) : AlterTableOperation, IElement
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
    public record ChangeColumn(Ident OldName, Ident NewName, DataType DataType, Sequence<ColumnOption> Options) : AlterTableOperation, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"CHANGE COLUMN {OldName} {NewName} {DataType}");
            if (Options.Any())
            {
                writer.WriteSql($" {Options.ToSqlDelimited(" ")}");
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
    /// Alter column table operation
    /// <example>
    /// <c>
    /// ALTER [ COLUMN ]
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="ColumnName">Column Name</param>
    /// <param name="Operation">Alter column operation</param>
    public record AlterColumn(Ident ColumnName, AlterColumnOperation Operation) : AlterTableOperation, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ALTER COLUMN {ColumnName} {Operation}");
        }
    }
    /// <summary>
    /// 
    /// </summary>
    /// <param name="Name"></param>
    public record SwapWith(ObjectName Name) : AlterTableOperation, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SWAP WITH {Name}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
