namespace SqlParser.Ast;

public abstract record Password : IWriteSql
{
    public record ValidPassword(Expression Expression) : Password, IElement;

    public record NullPassword : Password;

    public void ToSql(SqlTextWriter writer)
    {
        if (this is ValidPassword v)
        {
            writer.WriteSql($" PASSWORD {v.Expression}");
        }
        else if(this is NullPassword)
        {
            writer.WriteSql($" PASSWORD NULL");
        }
    }
}