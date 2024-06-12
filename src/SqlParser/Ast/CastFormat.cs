namespace SqlParser.Ast;

/// <summary>
/// Options for `CAST` / `TRY_CAST`
/// </summary>
public abstract record CastFormat : IWriteSql, IElement
{
    public record Value(Ast.Value Val) : CastFormat;

    public record ValueAtTimeZone(Ast.Value Val, Ast.Value TimeZone) : CastFormat;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Value v:
                writer.WriteSql($"{v.Val}");
                break;
            case ValueAtTimeZone tz:
                writer.WriteSql($"{tz.Val} AT TIME ZONE {tz.TimeZone}");
                break;
        }
    }
}
