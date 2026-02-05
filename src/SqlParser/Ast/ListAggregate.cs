namespace SqlParser.Ast;

// <summary>
// List aggregate
// </summary>
// <param name="Expression">Expression</param>
// <param name="Distinct">True if distinct</param>
// <param name="Separator">Aggregation separator expression</param>
// <param name="OnOverflow">Optional n overflow</param>
// <param name="WithGroup">Order by list with group</param>
//public record ListAggregate(Expression Expression, bool Distinct, Expression? Separator, ListAggOnOverflow? OnOverflow, Sequence<OrderByExpression> WithGroup) : IWriteSql, IElement
//{
//    public void ToSql(SqlTextWriter writer)
//    {
//        var distinct = Distinct ? "DISTINCT " : null;
//        writer.WriteSql($"LISTAGG({distinct}{Expression}");

//        if (Separator != null)
//        {
//            writer.WriteSql($", {Separator}");
//        }

//        if (OnOverflow != null)
//        {
//            writer.WriteSql($"{OnOverflow}");
//        }

//        writer.Write(')');

//        if (WithGroup.SafeAny())
//        {
//            writer.WriteSql($" WITHIN GROUP (ORDER BY {WithGroup})");
//        }
//    }
//}

/// <summary>
/// List aggregation on overflow
/// </summary>
public abstract record ListAggOnOverflow : IWriteSql, IElement
{
    /// <summary>
    /// Error on overflow
    /// </summary>
    public record Error : ListAggOnOverflow;
    /// <summary>
    /// Truncate on overflow
    /// </summary>
    public record Truncate : ListAggOnOverflow
    {
        public Expression? Filler { get; init; }
        public bool WithCount { get; init; }
    }

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("ON OVERFLOW");

        if (this is Error)
        {
            writer.Write(" ERROR");
        }
        else if (this is Truncate t)
        {
            writer.Write(" TRUNCATE");

            if (t.Filler != null)
            {
                writer.WriteSql($" {t.Filler}");
            }

            writer.Write(t.WithCount ? " WITH" : " WITHOUT");

            writer.Write(" COUNT");
        }
    }
}
