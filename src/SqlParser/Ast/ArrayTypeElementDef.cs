namespace SqlParser.Ast;

public abstract record ArrayElementTypeDef : IWriteSql
{
    public record None : ArrayElementTypeDef;

    public record AngleBracket(DataType DataType) : ArrayElementTypeDef;
    
    public record SquareBracket(DataType DataType) : ArrayElementTypeDef;
    
    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case AngleBracket a:
                writer.WriteSql($"{a.DataType}");
                break;

            case SquareBracket s:
                writer.WriteSql($"{s.DataType}");
                break;
        }
    }
}