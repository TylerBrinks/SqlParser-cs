namespace SqlParser.Ast;

public abstract record Deduplicate : IWriteSql, IElement
{
    public record All : Deduplicate;
    public record ByExpression(Expression Expression) : Deduplicate;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case All:
                writer.Write("DEDUPLICATE");
                break;

            case ByExpression b:
                writer.WriteSql($"DEDUPLICATE BY {b.Expression}");
                break;
        }
    }
}