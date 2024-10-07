namespace SqlParser.Ast;

public record IdentityProperty(Expression Seed, Expression Increment) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Seed}, {Increment}");
    }
}
