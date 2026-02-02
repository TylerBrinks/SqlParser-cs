namespace SqlParser.Ast;

/// <summary>
/// RAISE statement for PostgreSQL/Snowflake
/// </summary>
public abstract record RaiseStatement : IWriteSql, IElement
{
    /// <summary>
    /// RAISE with no parameters
    /// </summary>
    public record RaiseExceptionLevel(RaiseStatementLevel? Level, RaiseStatementValue? Value) : RaiseStatement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("RAISE");

            if (Level != null)
            {
                writer.WriteSql($" {Level}");
            }

            if (Value != null)
            {
                writer.WriteSql($" {Value}");
            }
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Raise statement level
/// </summary>
public enum RaiseStatementLevel
{
    Debug,
    Log,
    Info,
    Notice,
    Warning,
    Exception
}

/// <summary>
/// Raise statement value
/// </summary>
public abstract record RaiseStatementValue : IWriteSql, IElement
{
    public record UsingMessage(Expression Message) : RaiseStatementValue
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"USING MESSAGE = {Message}");
        }
    }

    public record Format(Value Format, Sequence<Expression>? Arguments = null) : RaiseStatementValue
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Format}");

            if (Arguments.SafeAny())
            {
                writer.WriteSql($", {Arguments.ToSqlDelimited()}");
            }
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
