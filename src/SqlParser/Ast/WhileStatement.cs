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

        foreach (var stmt in Body)
        {
            writer.WriteSql($"{stmt}; ");
        }

        writer.Write("END WHILE");

        if (Label != null)
        {
            writer.WriteSql($" {Label}");
        }
    }
}
