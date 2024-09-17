namespace SqlParser.Ast;

public record TriggerReferencing(bool IsAs, ObjectName TransitionRelationName, TriggerReferencingType ReferType) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        var isAs = IsAs ? " AS" : string.Empty;

        writer.WriteSql($"{ReferType}{isAs} {TransitionRelationName}");
    }
}

public abstract record TriggerEvent : IWriteSql, IElement
{
    public record Insert : TriggerEvent;
    public record Update(Sequence<Ident>? Columns = null) : TriggerEvent;
    public record Delete : TriggerEvent;
    public record Truncate : TriggerEvent;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Insert:
                writer.Write("INSERT");
                break;

            case Update u:
                writer.Write("UPDATE");
                if (u.Columns.SafeAny())
                {
                    writer.WriteSql($" OF {u.Columns.ToSqlDelimited()}");
                }
                break;

            case Delete:
                writer.Write("DELETE");
                break;

            case Truncate:
                writer.Write("TRUNCATE");
                break;

        }
    }
}

public record TriggerExecBody(TriggerExecBodyType ExecType, Statement.FunctionDesc FunctionDescription) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{ExecType} {FunctionDescription}");
    }
}