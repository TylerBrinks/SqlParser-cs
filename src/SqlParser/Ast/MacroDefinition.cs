namespace SqlParser.Ast;

public abstract record MacroDefinition : IWriteSql, IElement
{
    public record MacroExpression(Expression Expression) : MacroDefinition;

    public record MacroTable(Query Query) : MacroDefinition;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case MacroExpression e:
                writer.WriteSql($"{e.Expression}");
                break;
            case MacroTable t:
                writer.WriteSql($"{t.Query}");
                break;
        }
    }
}