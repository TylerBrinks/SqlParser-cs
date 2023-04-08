namespace SqlParser.Ast;

/// <summary>
/// A Hive LATERAL VIEW with potential column aliases
/// </summary>
/// <param name="Expression">Vie expression</param>
public record LateralView([property: Visit(0)] Expression Expression) : IWriteSql, IElement
{
    [Visit(1)]
    public ObjectName? LateralViewName { get; init; }
    public Sequence<Ident?>? LateralColAlias { get; init; }
    public bool Outer { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        var outer = Outer ? " OUTER" : null;
        writer.WriteSql($" LATERAL VIEW{outer} {Expression} {LateralViewName}");

        if (LateralColAlias.SafeAny())
        {
            writer.WriteSql($" AS {LateralColAlias}");
        }
    }
}