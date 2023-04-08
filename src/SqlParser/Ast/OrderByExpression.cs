namespace SqlParser.Ast;

/// <summary>
/// Order by expression
/// </summary>
/// <param name="Expression">Expression</param>
/// <param name="Asc">Ascending if true; descending if false</param>
/// <param name="NullsFirst">Nulls first if true; Nulls last if false</param>
public record OrderByExpression(Expression Expression, bool? Asc = null, bool? NullsFirst = null) : IWriteSql, IElement
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
    }
}