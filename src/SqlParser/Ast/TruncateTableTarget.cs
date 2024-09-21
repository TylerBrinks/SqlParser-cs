namespace SqlParser.Ast;

public record TruncateTableTarget(ObjectName Name) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name}");
    }
}