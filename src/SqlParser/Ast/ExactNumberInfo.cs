namespace SqlParser.Ast;

public abstract record ExactNumberInfo : IWriteSql
{
    /// <summary>
    /// No additional information e.g. `DECIMAL`
    /// </summary>
    public record None : ExactNumberInfo;
    /// <summary>
    /// Only precision information e.g. `DECIMAL(10)`
    /// </summary>
    /// <param name="Length">Length</param>
    public record Precision(ulong Length) : ExactNumberInfo;
    /// <summary>
    /// Precision and scale information e.g. `DECIMAL(10,2)`
    /// </summary>
    /// <param name="Length">Length</param>
    /// <param name="Scale">Scale</param>
    public record PrecisionAndScale(ulong Length, ulong Scale) : ExactNumberInfo;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Precision p:
                writer.Write($"({p.Length})");
                break;

            case PrecisionAndScale ps:
                writer.Write($"({ps.Length},{ps.Scale})");
                break;
        }
    }
}