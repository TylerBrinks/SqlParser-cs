namespace SqlParser.Ast;

/// <summary>
/// Procedural WHILE statement
/// </summary>
public record WhileStatement(
    Expression Condition,
    Sequence<Statement> Body) : IWriteSql, IElement
{
    public Ident? Label { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        if (Label != null)
        {
            writer.WriteSql($"{Label}: ");
        }

        writer.WriteSql($"WHILE {Condition} DO ");

        for (var i = 0; i < Body.Count; i++)
        {
            if (i > 0)
            {
                writer.Write("; ");
            }
            writer.WriteSql($"{Body[i]}");
        }

        writer.Write(" END WHILE");

        if (Label != null)
        {
            writer.WriteSql($" {Label}");
        }
    }
}
