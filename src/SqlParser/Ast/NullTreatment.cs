namespace SqlParser.Ast;

public abstract record NullTreatment : IWriteSql, IElement
{
    public record IgnoreNulls : NullTreatment;

    public record RespectNulls : NullTreatment;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case IgnoreNulls:
                writer.Write("IGNORE NULLS");
                break;

            case RespectNulls:
                writer.Write("RESPECT NULLS");
                break;
        }
    }
}