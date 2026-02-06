namespace SqlParser.Ast;

/// <summary>
/// EXECUTE IMMEDIATE statement
/// </summary>
public record ExecuteImmediateStatement(Expression Statement) : IWriteSql, IElement
{
    public Sequence<ExecuteImmediateInto>? Into { get; init; }
    public Sequence<ExecuteImmediateUsing>? Using { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"EXECUTE IMMEDIATE {Statement}");

        if (Into.SafeAny())
        {
            writer.WriteSql($" INTO {Into.ToSqlDelimited()}");
        }

        if (Using.SafeAny())
        {
            writer.WriteSql($" USING {Using.ToSqlDelimited()}");
        }
    }
}

/// <summary>
/// INTO clause target for EXECUTE IMMEDIATE
/// </summary>
public record ExecuteImmediateInto(Ident Variable) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Variable}");
    }
}

/// <summary>
/// USING clause value for EXECUTE IMMEDIATE
/// </summary>
public record ExecuteImmediateUsing(Expression Value) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Value}");
    }
}
