namespace SqlParser.Ast;

public record OrderBy(Sequence<OrderByExpression>? Expressions, Interpolate? Interpolate) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"ORDER BY");

        if (Expressions.SafeAny())
        {
            writer.Write(" ");
            writer.WriteDelimited(Expressions, Constants.SpacedComma);
        }

        if (Interpolate != null)
        {
            if (Interpolate.Expressions != null)
            {
                writer.WriteSql($" INTERPOLATE ({Interpolate.Expressions.ToSqlDelimited()})");
            }
            else
            {
                writer.WriteSql($" INTERPOLATE");
            }
        }
    }
}