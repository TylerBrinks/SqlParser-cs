namespace SqlParser.Ast;

public record MacroArg(Ident Name, Expression? DefaultExpression = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name}");

        if (DefaultExpression != null)
        {
            writer.WriteSql($" := {DefaultExpression}");
        }
    }
}