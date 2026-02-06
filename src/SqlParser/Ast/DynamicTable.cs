namespace SqlParser.Ast;

/// <summary>
/// Dynamic table target lag specification
/// </summary>
public abstract record DynamicTableLag : IWriteSql, IElement
{
    /// <summary>
    /// TARGET_LAG = 'interval'
    /// </summary>
    public record IntervalLag(string Interval) : DynamicTableLag
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"TARGET_LAG = '{Interval}'");
        }
    }

    /// <summary>
    /// TARGET_LAG = DOWNSTREAM
    /// </summary>
    public record Downstream : DynamicTableLag
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TARGET_LAG = DOWNSTREAM");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Dynamic table refresh mode
/// </summary>
public enum DynamicTableRefreshMode
{
    Auto,
    Full,
    Incremental
}

/// <summary>
/// Dynamic table initialize setting
/// </summary>
public enum DynamicTableInitialize
{
    OnCreate,
    OnSchedule
}
