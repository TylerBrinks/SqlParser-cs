namespace SqlParser.Ast;

/// <summary>
/// Procedural CASE statement (different from CASE expression)
/// </summary>
public record CaseStatement(Sequence<CaseStatementBranch> Branches) : IWriteSql, IElement
{
    public Expression? Operand { get; init; }
    public Sequence<Statement>? ElseBlock { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("CASE");

        if (Operand != null)
        {
            writer.WriteSql($" {Operand}");
        }

        foreach (var branch in Branches)
        {
            writer.WriteSql($" WHEN {branch.Condition} THEN ");
            foreach (var stmt in branch.Statements)
            {
                writer.WriteSql($"{stmt}; ");
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

        writer.Write("END CASE");
    }
}

/// <summary>
/// Branch of a CASE statement
/// </summary>
public record CaseStatementBranch(
    Expression Condition,
    Sequence<Statement> Statements);
