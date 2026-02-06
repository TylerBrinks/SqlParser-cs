namespace SqlParser.Ast;

public abstract record NullTreatmentType : IElement, IWriteSql
{
    public record FunctionArg(NullTreatment Treatment) : NullTreatmentType;
    public record AfterFunction(NullTreatment Treatment) : NullTreatmentType;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case FunctionArg f:
                writer.WriteSql($"{f.Treatment}");
                break;

            case AfterFunction a:
                writer.WriteSql($"{a.Treatment}");
                break;
        }
    }
}
