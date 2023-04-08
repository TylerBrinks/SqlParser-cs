namespace SqlParser.Ast;

/// <summary>
/// Fetch operation
/// </summary>
/// <param name="Quantity">Fetch quantity</param>
/// <param name="WithTies">With ties flag</param>
/// <param name="Percent">Fetch is percentage</param>
public record Fetch(Expression? Quantity = null, bool WithTies = false, bool Percent = false) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        var extension = WithTies ? "WITH TIES" : "ONLY";
        if (Quantity != null)
        {
            var percent = Percent ? " PERCENT" : null;
            writer.Write($"FETCH FIRST {Quantity.ToSql()}{percent} ROWS {extension}");
        }
        else
        {
            writer.Write($"FETCH FIRST ROWS {extension}");
        }
    }
}

/// <summary>
/// Fetch direction
/// </summary>
public abstract record FetchDirection : IWriteSql
{
    /// <summary>
    /// Fetch with limit
    /// </summary>
    /// <param name="Limit"></param>
    public abstract record LimitedFetchDirection(Value Limit) : FetchDirection;

    /// <summary>
    /// Fetch count 
    /// </summary>
    public record Count(Value Limit) : LimitedFetchDirection(Limit);
    /// <summary>
    /// Fetch next
    /// </summary>
    public record Next : FetchDirection;
    /// <summary>
    /// Fetch prior
    /// </summary>
    public record Prior : FetchDirection;
    /// <summary>
    /// Fetch first
    /// </summary>
    public record First : FetchDirection;
    /// <summary>
    /// Fetch last
    /// </summary>
    public record Last : FetchDirection;
    /// <summary>
    /// Fetch absolute
    /// </summary>
    public record Absolute(Value Limit) : LimitedFetchDirection(Limit);
    /// <summary>
    /// 
    /// </summary>
    public record Relative(Value Limit) : LimitedFetchDirection(Limit);
    /// <summary>
    /// Fetch all
    /// </summary>
    public record All : FetchDirection;
    /// <summary>
    /// Fetch forward
    /// </summary>
    public record Forward(Value Limit) : LimitedFetchDirection(Limit);
    /// <summary>
    /// Fetch forward all
    /// </summary>
    public record ForwardAll : FetchDirection;
    /// <summary>
    /// Fetch backward
    /// </summary>
    public record Backward(Value Limit) : LimitedFetchDirection(Limit);
    /// <summary>
    /// Fetch backward all
    /// </summary>
    public record BackwardAll : FetchDirection;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Count c:
                c.Limit.ToSql(writer);
                break;

            case Next:
                writer.Write("NEXT");
                break;

            case Prior:
                writer.Write("PRIOR");
                break;

            case First:
                writer.Write("FIRST");
                break;

            case Last:
                writer.Write("LAST");
                break;
            case Absolute a:
                writer.WriteSql($"ABSOLUTE {a.Limit}");
                break;
            case Relative r:
                writer.WriteSql($"RELATIVE {r.Limit}");
                break;
            case All:
                writer.Write("ALL");
                break;
            case Forward f:
                writer.Write("FORWARD");
                
                //TODO once optional is supported
                //if (f.Limit != null)
                //{
                    writer.WriteSql($" {f.Limit}");
                //}
                break;
            case ForwardAll:
                writer.Write("FORWARD ALL");
                break;
            case Backward b:
                writer.Write("BACKWARD");

                //TODO once optional is supported
                //if (f.Limit != null)
                //{
                    writer.WriteSql($" {b.Limit}");
                //}
                break;
            case BackwardAll:
                writer.Write("BACKWARD ALL");
                break;
        }
    }
}