namespace SqlParser.Ast;

/// <summary>
/// SQL Server PRINT statement
/// </summary>
public record PrintStatement(Expression Message) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"PRINT {Message}");
    }
}
