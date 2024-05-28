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

public record MergeInsertExpression(Sequence<Ident> Columns, MergeInsertKind Kind) : IWriteSql
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

public abstract record MergeInsertKind : IWriteSql
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

public abstract record MergeAction : IWriteSql
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

//public abstract record MergeClause : IWriteSql, IElement
//{
//    /// <summary>
//    /// Matched update clause
//    /// </summary>
//    /// <param name="Predicate">Expression predicate</param>
//    /// <param name="Assignments">Merge update assignments</param>
//    public record MatchedUpdate(Sequence<Statement.Assignment> Assignments, Expression? Predicate = null) : MergeClause
//    {
//        public override void ToSql(SqlTextWriter writer)
//        {
//            writer.Write("WHEN MATCHED");

//            if(Predicate != null) 
//            {
//                writer.WriteSql($" AND {Predicate}");
//            }

//            writer.WriteSql($" THEN UPDATE SET {Assignments}");
//        }
//    }
//    /// <summary>
//    /// Matched delete clause
//    /// </summary>
//    /// <param name="Predicate">Delete expression</param>
//    public record MatchedDelete(Expression? Predicate = null) : MergeClause
//    {
//        public override void ToSql(SqlTextWriter writer)
//        {
//            writer.Write("WHEN MATCHED");
        
//            if (Predicate !=null)
//            {
//                writer.WriteSql($" AND {Predicate}");
//            }

//            writer.Write(" THEN DELETE");
//        }
//    }
//    /// <summary>
//    /// Not matched update clause
//    /// </summary>
//    /// <param name="Predicate">Expression predicate</param>
//    /// <param name="Columns">Columns</param>
//    /// <param name="Values">Values</param>
//    public record NotMatched(Sequence<Ident> Columns, Values Values, Expression? Predicate = null) : MergeClause
//    {
//        public override void ToSql(SqlTextWriter writer)
//        {
//            writer.Write("WHEN NOT MATCHED");
//            if (Predicate != null)
//            {
//                writer.WriteSql($" AND {Predicate}");
//            }

//            writer.WriteSql($" THEN INSERT ({Columns}) {Values}");
//        }
//    }

//    public abstract void ToSql(SqlTextWriter writer);
//}