namespace SqlParser.Ast;

/// <summary>
/// TABLESAMPLE kind/method
/// </summary>
public abstract record TableSampleKind : IWriteSql, IElement
{
    /// <summary>
    /// TABLESAMPLE method (n PERCENT)
    /// </summary>
    public record Percent(TableSampleMethod? Method, Expression Percentage) : TableSampleKind
    {
        public bool? RepeatableSeed { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TABLESAMPLE ");

            if (Method != null)
            {
                writer.WriteSql($"{Method} ");
            }

            writer.WriteSql($"({Percentage} PERCENT)");

            if (RepeatableSeed.HasValue)
            {
                writer.Write($" REPEATABLE ({RepeatableSeed.Value})");
            }
        }
    }

    /// <summary>
    /// TABLESAMPLE method (n ROWS)
    /// </summary>
    public record Rows(TableSampleMethod? Method, Expression RowCount) : TableSampleKind
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TABLESAMPLE ");

            if (Method != null)
            {
                writer.WriteSql($"{Method} ");
            }

            writer.WriteSql($"({RowCount} ROWS)");
        }
    }

    /// <summary>
    /// TABLESAMPLE BUCKET x OUT OF y
    /// </summary>
    public record Bucket(Expression BucketNumber, Expression TotalBuckets) : TableSampleKind
    {
        public Expression? On { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"TABLESAMPLE (BUCKET {BucketNumber} OUT OF {TotalBuckets}");

            if (On != null)
            {
                writer.WriteSql($" ON {On}");
            }

            writer.Write(")");
        }
    }

    /// <summary>
    /// TABLESAMPLE with raw value (no PERCENT/ROWS suffix, e.g., Hive byte-size 100M)
    /// </summary>
    public record Raw(TableSampleMethod? Method, Expression Value) : TableSampleKind
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TABLESAMPLE ");

            if (Method != null)
            {
                writer.WriteSql($"{Method} ");
            }

            writer.WriteSql($"({Value})");
        }
    }

    /// <summary>
    /// ClickHouse SAMPLE clause (no parentheses)
    /// </summary>
    public record Sample(Expression Value) : TableSampleKind
    {
        public Expression? Offset { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SAMPLE {Value}");

            if (Offset != null)
            {
                writer.WriteSql($" OFFSET {Offset}");
            }
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// TABLESAMPLE sampling method
/// </summary>
public enum TableSampleMethod
{
    Bernoulli,
    System,
    Row
}
