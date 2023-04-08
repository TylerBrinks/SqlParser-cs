namespace SqlParser.Ast;

/// <summary>
/// Alter index operations
/// </summary>
public abstract record AlterIndexOperation : IWriteSql
{
    /// <summary>
    /// Rename index alter operation
    /// </summary>
    /// <param name="Name">Object name</param>
    public record RenameIndex(ObjectName Name) : AlterIndexOperation, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"RENAME TO {Name}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}