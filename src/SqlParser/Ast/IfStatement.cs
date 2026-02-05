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

        for (var i = 0; i < ThenBlock.Count; i++)
        {
            if (i > 0)
            {
                writer.Write("; ");
            }
            writer.WriteSql($"{ThenBlock[i]}");
        }

        writer.Write(" ");

        if (ElseIfs != null)
        {
            foreach (var elseif in ElseIfs)
            {
                writer.WriteSql($"ELSEIF {elseif.Condition} THEN ");
                for (var i = 0; i < elseif.Statements.Count; i++)
                {
                    if (i > 0)
                    {
                        writer.Write("; ");
                    }
                    writer.WriteSql($"{elseif.Statements[i]}");
                }
                writer.Write(" ");
            }
        }

        if (ElseBlock != null)
        {
            writer.Write("ELSE ");
            for (var i = 0; i < ElseBlock.Count; i++)
            {
                if (i > 0)
                {
                    writer.Write("; ");
                }
                writer.WriteSql($"{ElseBlock[i]}");
            }
            writer.Write(" ");
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
