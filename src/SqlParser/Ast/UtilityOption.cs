namespace SqlParser.Ast;

public record UtilityOption(Ident Name, Expression? Arg = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        if (Arg != null)
        {
            writer.WriteSql($"{Name} {Arg}");
        }
        else
        {
            writer.WriteSql($"{Name}");
        }
    }
}
