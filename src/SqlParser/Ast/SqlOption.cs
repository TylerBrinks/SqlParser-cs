namespace SqlParser.Ast;

/// <summary>
/// Sql option
/// </summary>
/// <param name="Name">Name identifier</param>
/// <param name="Value">Value</param>
public record SqlOption(Ident Name, Value Value) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name} = {Value}");
    }
}