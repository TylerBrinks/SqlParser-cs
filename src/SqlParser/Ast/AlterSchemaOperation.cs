namespace SqlParser.Ast;

/// <summary>
/// ALTER SCHEMA operation
/// </summary>
public abstract record AlterSchemaOperation : IWriteSql, IElement
{
    /// <summary>
    /// RENAME TO new_name
    /// </summary>
    public record RenameTo(Ident NewName) : AlterSchemaOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME TO {NewName}");
        }
    }

    /// <summary>
    /// OWNER TO new_owner
    /// </summary>
    public record OwnerTo(Owner NewOwner) : AlterSchemaOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"OWNER TO {NewOwner}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
