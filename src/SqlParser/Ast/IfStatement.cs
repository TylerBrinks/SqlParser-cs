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
    public IfStatementSyntax Syntax { get; init; } = IfStatementSyntax.Standard;

    public void ToSql(SqlTextWriter writer)
    {
        if (Syntax == IfStatementSyntax.MsSql)
        {
            WriteMsSql(writer);
            return;
        }

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

    private void WriteMsSql(SqlTextWriter writer)
    {
        writer.WriteSql($"IF {Condition} ");
        WriteMsSqlBranch(writer, ThenBlock);

        if (ElseIfs != null)
        {
            foreach (var elseIf in ElseIfs)
            {
                writer.WriteSql($" ELSE IF {elseIf.Condition} ");
                WriteMsSqlBranch(writer, elseIf.Statements);
            }
        }

        if (ElseBlock != null)
        {
            writer.Write(" ELSE ");
            WriteMsSqlBranch(writer, ElseBlock);
        }
    }

    private static void WriteMsSqlBranch(SqlTextWriter writer, Sequence<Statement> statements)
    {
        if (!statements.SafeAny())
        {
            writer.Write("BEGIN END");
            return;
        }

        if (statements.Count == 1)
        {
            writer.WriteSql($"{statements[0]}");
            return;
        }

        writer.Write("BEGIN ");
        for (var i = 0; i < statements.Count; i++)
        {
            writer.WriteSql($"{statements[i]}");
            writer.Write(";");
            if (i < statements.Count - 1)
            {
                writer.Write(" ");
            }
        }
        writer.Write(" END");
    }
}

/// <summary>
/// ELSEIF clause of an IF statement
/// </summary>
public record IfStatementElseIf(
    Expression Condition,
    Sequence<Statement> Statements);

public enum IfStatementSyntax
{
    Standard,
    MsSql
}
