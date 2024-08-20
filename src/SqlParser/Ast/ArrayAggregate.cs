namespace SqlParser.Ast;

/// <summary>
/// An ARRAY_AGG invocation
/// 
/// ORDER BY position is defined differently for BigQuery, Postgres and Snowflake.
/// <example>
/// <c>
/// ARRAY_AGG( [ DISTINCT ] Expression [ORDER BY expr] [LIMIT n] )
/// Or
/// ARRAY_AGG( [ DISTINCT ] Expression ) [ WITHIN GROUP ( ORDER BY Expression ) ]
/// </c>
/// </example>
/// </summary>
/// <param name="Expression"></param>
public record ArrayAggregate([property:Visit(0)]Expression Expression) : IWriteSql, IElement
{
    [Visit(1)]
    public Sequence<OrderByExpression>? OrderBy { get; set; }
    [Visit(2)] 
    public Expression? Limit { get; init; }
    public bool Distinct { get; init; }
    public bool WithinGroup { get; init; }
    public void ToSql(SqlTextWriter writer)
    {
        var distinct = Distinct ? "DISTINCT " : null;
        writer.WriteSql($"ARRAY_AGG({distinct}{Expression}");

        if (!WithinGroup)
        {
            if (OrderBy != null)
            {
                writer.WriteSql($" ORDER BY {OrderBy.ToSqlDelimited()}");
            }
            if (Limit != null)
            {
                writer.WriteSql($" LIMIT {Limit}");
            }
        }

        writer.Write(')');

        if (WithinGroup)
        {
            if (OrderBy != null)
            {
                writer.WriteSql($" WITHIN GROUP (ORDER BY {OrderBy.ToSqlDelimited()})");
            }
        }
    }
}

/// <summary>
/// Represents an Array Expression
/// <example>
/// <c>
/// `ARRAY[..]`, or `[..]`
/// </c>
/// </example>
/// </summary>
/// <param name="Element">The list of expressions between brackets</param>
/// <param name="Named">true for ARRAY[..], false for [..]</param>
public record ArrayExpression(Sequence<Expression> Element, bool Named = false) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        var named = Named ? "ARRAY" : null;

        writer.WriteSql($"{named}[{Element}]");
    }
}