namespace SqlParser.Ast;

/// <summary>
/// Procedural IF statement
/// </summary>
public record IfStatement(
    Expression Condition,
    Sequence<Statement> ThenBlock) : IWriteSql, IElement
{
    public Sequence<IfStatementElseIf>? ElseIfs { get; init; }
    public Sequence<Statement>? ElseBlock { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"IF {Condition} THEN ");

        foreach (var stmt in ThenBlock)
        {
            writer.WriteSql($"{stmt}; ");
        }

        if (ElseIfs != null)
        {
            foreach (var elseif in ElseIfs)
            {
                writer.WriteSql($"ELSEIF {elseif.Condition} THEN ");
                foreach (var stmt in elseif.Statements)
                {
                    writer.WriteSql($"{stmt}; ");
                }
            }
        }

        if (ElseBlock != null)
        {
            writer.Write("ELSE ");
            foreach (var stmt in ElseBlock)
            {
                writer.WriteSql($"{stmt}; ");
            }
        }

        writer.Write("END IF");
    }
}

/// <summary>
/// ELSEIF clause of an IF statement
/// </summary>
public record IfStatementElseIf(
    Expression Condition,
    Sequence<Statement> Statements);
