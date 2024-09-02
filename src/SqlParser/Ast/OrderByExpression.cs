namespace SqlParser.Ast;

/// <summary>
/// Order by expression
/// </summary>
/// <param name="Expression">Expression</param>
/// <param name="Asc">Ascending if true; descending if false</param>
/// <param name="NullsFirst">Nulls first if true; Nulls last if false</param>
public record OrderByExpression(
    Expression Expression, 
    bool? Asc = null, 
    bool? NullsFirst = null,
    WithFill? WithFill = null
) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        Expression.ToSql(writer);

        if (Asc.HasValue)
        {
            writer.Write(Asc.Value ? " ASC" : " DESC");
        }

        if (NullsFirst.HasValue)
        {
            writer.Write(NullsFirst.Value ? " NULLS FIRST" : " NULLS LAST");
        }

        if (WithFill != null)
        {
            writer.WriteSql($" {WithFill}");
        }
    }
}

public record WithFill(Expression? From = null, Expression? To = null, Expression? Step = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("WITH FILL");

        if (From != null)
        {
            writer.WriteSql($" FROM {From}");
        }

        if (To != null)
        {
            writer.WriteSql($" TO {To}");
        }

        if (Step != null)
        {
            writer.WriteSql($" STEP {Step}");
        }
    }
}

public record InterpolateExpression(Ident Column, Expression? Expression) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Column}");
        if (Expression != null)
        {
            writer.WriteSql($" AS {Expression}");
        }
    }
}

public record Interpolate(Sequence<InterpolateExpression>? Expressions) : IElement;