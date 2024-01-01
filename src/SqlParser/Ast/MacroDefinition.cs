namespace SqlParser.Ast;

public abstract record MacroDefinition : IWriteSql
{
    public record MacroExpression(Expression Expression) : MacroDefinition;

    public record MacroTable(Query Query) : MacroDefinition;

    public void ToSql(SqlTextWriter writer)
    {
        if (this is MacroExpression e)
        {
            writer.WriteSql($"{e.Expression}");
        }
        else if (this is MacroTable t)
        {
            writer.WriteSql($"{t.Query}");
        }
    }
}