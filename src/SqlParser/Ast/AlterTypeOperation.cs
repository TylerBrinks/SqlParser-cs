namespace SqlParser.Ast;

/// <summary>
/// Operation for ALTER TYPE statement
/// </summary>
public abstract record AlterTypeOperation : IWriteSql, IElement
{
    /// <summary>
    /// ADD VALUE 'new_value' [ BEFORE | AFTER 'existing_value' ]
    /// </summary>
    public record AddValue(string NewValue) : AlterTypeOperation
    {
        public bool IfNotExists { get; init; }
        public string? BeforeValue { get; init; }
        public string? AfterValue { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("ADD VALUE ");

            if (IfNotExists)
            {
                writer.Write("IF NOT EXISTS ");
            }

            writer.Write($"'{NewValue}'");

            if (BeforeValue != null)
            {
                writer.Write($" BEFORE '{BeforeValue}'");
            }
            else if (AfterValue != null)
            {
                writer.Write($" AFTER '{AfterValue}'");
            }
        }
    }

    /// <summary>
    /// RENAME TO new_name
    /// </summary>
    public record RenameTo(ObjectName NewName) : AlterTypeOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME TO {NewName}");
        }
    }

    /// <summary>
    /// RENAME VALUE 'old_value' TO 'new_value'
    /// </summary>
    public record RenameValue(string OldValue, string NewValue) : AlterTypeOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"RENAME VALUE '{OldValue}' TO '{NewValue}'");
        }
    }

    /// <summary>
    /// OWNER TO new_owner
    /// </summary>
    public record OwnerTo(Ident NewOwner) : AlterTypeOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"OWNER TO {NewOwner}");
        }
    }

    /// <summary>
    /// SET SCHEMA new_schema
    /// </summary>
    public record SetSchema(ObjectName NewSchema) : AlterTypeOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SET SCHEMA {NewSchema}");
        }
    }

    /// <summary>
    /// ADD ATTRIBUTE attr_name data_type [ COLLATE collation ]
    /// </summary>
    public record AddAttribute(Ident Name, DataType DataType) : AlterTypeOperation
    {
        public ObjectName? Collation { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ADD ATTRIBUTE {Name} {DataType}");

            if (Collation != null)
            {
                writer.WriteSql($" COLLATE {Collation}");
            }
        }
    }

    /// <summary>
    /// DROP ATTRIBUTE attr_name [ IF EXISTS ]
    /// </summary>
    public record DropAttribute(Ident Name) : AlterTypeOperation
    {
        public bool IfExists { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DROP ATTRIBUTE ");

            if (IfExists)
            {
                writer.Write("IF EXISTS ");
            }

            writer.WriteSql($"{Name}");
        }
    }

    /// <summary>
    /// ALTER ATTRIBUTE attr_name SET DATA TYPE data_type [ COLLATE collation ]
    /// </summary>
    public record AlterAttribute(Ident Name, DataType DataType) : AlterTypeOperation
    {
        public ObjectName? Collation { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ALTER ATTRIBUTE {Name} SET DATA TYPE {DataType}");

            if (Collation != null)
            {
                writer.WriteSql($" COLLATE {Collation}");
            }
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
