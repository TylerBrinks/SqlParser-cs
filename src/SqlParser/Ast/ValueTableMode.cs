namespace SqlParser.Ast;

public abstract record ValueTableMode : IWriteSql, IElement
{
    public record AsStruct : ValueTableMode { }
    public record AsValue : ValueTableMode { }


    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case AsStruct:
                writer.Write("AS STRUCT");
                break;
            case AsValue:
                writer.Write("AS VALUE");
                break;
        }
    }
}
