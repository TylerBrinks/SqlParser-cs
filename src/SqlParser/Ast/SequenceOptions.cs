namespace SqlParser.Ast;

/// <summary>
/// Can use to describe options in create sequence or table column type identity
/// <example>
/// <c>
/// [ INCREMENT [ BY ] increment ]
/// [ MINVALUE minvalue | NO MINVALUE ] [ MAXVALUE maxvalue | NO MAXVALUE ]
/// [ START [ WITH ] start ] [ CACHE cache ] [ [ NO ] CYCLE ]
/// </c>
/// </example>
/// </summary>
public abstract record SequenceOptions : IWriteSql, IElement
{
    /// <summary>
    /// By by sequence
    /// </summary>
    /// <param name="Increment">Expression</param>
    /// <param name="By">True to increment</param>
    public record IncrementBy(Expression Increment, bool By) : SequenceOptions
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var by = By ? " BY" : null;

            writer.WriteSql($" INCREMENT{by} {Increment}");
        }
    }

    /// <summary>
    /// Min value sequence
    /// </summary>
    /// <param name="Value">Min value</param>
    public record MinValue(MinMaxValue Value) : SequenceOptions
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Value is MinMaxValue.Some)
            {
                writer.WriteSql($" MINVALUE {Value}");
            }
            else if (Value is MinMaxValue.None)
            {
                writer.Write(" NO MINVALUE");
            }
        }
    }
    /// <summary>
    /// Max value sequence
    /// </summary>
    /// <param name="Value">Max value</param>
    public record MaxValue(MinMaxValue Value) : SequenceOptions
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Value is MinMaxValue.Some)
            {
                writer.WriteSql($" MAXVALUE {Value}");
            }
            else if (Value is MinMaxValue.None)
            {
                writer.Write(" NO MAXVALUE");
            }
        }
    }
    /// <summary>
    /// Starts with sequence
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="With">True if start</param>
    public record StartWith(Expression Expression, bool With) : SequenceOptions
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var with = With ? " WITH" : null;
            writer.WriteSql($" START{with} {Expression}");
        }
    }
    /// <summary>
    /// Cache sequence
    /// </summary>
    /// <param name="Expression">Cycle expression</param>
    public record Cache(Expression Expression) : SequenceOptions
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($" CACHE {Expression}");
        }
    }
    /// <summary>
    /// Cycle sequence
    /// </summary>
    /// <param name="ShouldCycle">True if cycling</param>
    public record Cycle(bool ShouldCycle) : SequenceOptions
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var cycle = ShouldCycle ? "NO " : null;
            writer.WriteSql($" {cycle}CYCLE");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}