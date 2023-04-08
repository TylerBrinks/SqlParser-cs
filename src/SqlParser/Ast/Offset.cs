namespace SqlParser.Ast;

/// <summary>
/// Offset expression
/// </summary>
/// <param name="Value">Expression</param>
/// <param name="Rows">Offset rows type</param>
public record Offset(Expression Value, OffsetRows Rows) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"OFFSET {Value}");

        if (Rows != OffsetRows.None)
        {
            writer.WriteSql($" {Rows}");
        }
    }
}