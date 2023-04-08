namespace SqlParser.Ast;

/// <summary>
/// Min or max value
/// </summary>
public abstract record MinMaxValue
{
    /// <summary>
    /// Clause is not specified
    /// </summary>
    public record Empty : MinMaxValue;
    /// <summary>
    /// NO minvalue, no maxvalue
    /// </summary>
    public record None : MinMaxValue;
    /// <summary>
    /// Minimum or maximum value
    /// <example>
    /// <c>
    /// MINVALUE Expression / MAXVALUE expr
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Min/Max value expression</param>
    public record Some(Expression Expression) : MinMaxValue, IWriteSql, IElement
    {
        public void ToSql(SqlTextWriter writer)
        {
            Expression.ToSql(writer);
        }
    }
}