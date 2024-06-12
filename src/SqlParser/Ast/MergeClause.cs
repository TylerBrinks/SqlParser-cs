namespace SqlParser.Ast;

/// <summary>
/// Merge clause
/// </summary>
public record MergeClause(MergeClauseKind ClauseKind, MergeAction Action, Expression? Predicate = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"WHEN {ClauseKind}");

        if (Predicate != null)
        {
            writer.WriteSql($" AND {Predicate}");
        }

        writer.WriteSql($" THEN {Action}");
    }
}

public record MergeInsertExpression(Sequence<Ident> Columns, MergeInsertKind Kind) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        if (Columns.SafeAny())
        {
            writer.WriteSql($"({Columns.ToSqlDelimited()}) ");
        }

        writer.WriteSql($"{Kind}");
    }
}

public abstract record MergeInsertKind : IWriteSql, IElement
{
    public record Values(Ast.Values? InsertValues) : MergeInsertKind;

    public record Row : MergeInsertKind;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Values v:
                writer.WriteSql($"{v.InsertValues}");
                break;

            case Row:
                writer.Write("ROW");
                break;
        }
    }
}

public abstract record MergeAction : IWriteSql, IElement
{
    public record Insert(MergeInsertExpression Expression) : MergeAction;

    public record Update(Sequence<Statement.Assignment>? Assignments = null) : MergeAction;

    public record Delete : MergeAction;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Insert i:
                writer.WriteSql($"INSERT {i.Expression}");
                break;

            case Update u:
                writer.WriteSql($"UPDATE SET {u.Assignments.ToSqlDelimited()}");
                break;

            case Delete:
                writer.Write("DELETE");
                break;
        }
    }
}