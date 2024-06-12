namespace SqlParser.Ast;

public abstract record Password : IWriteSql, IElement
{
    public record ValidPassword(Expression Expression) : Password, IElement;

    public record NullPassword : Password;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case ValidPassword v:
                writer.WriteSql($" PASSWORD {v.Expression}");
                break;

            case NullPassword:
                writer.WriteSql($" PASSWORD NULL");
                break;
        }
    }
}