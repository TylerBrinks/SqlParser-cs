namespace SqlParser.Ast;

/// <summary>
/// Merge clause
/// </summary>
public abstract record MergeClause : IWriteSql, IElement
{
    /// <summary>
    /// Matched update clause
    /// </summary>
    /// <param name="Predicate">Expression predicate</param>
    /// <param name="Assignments">Merge update assignments</param>
    public record MatchedUpdate(Sequence<Statement.Assignment> Assignments, Expression? Predicate = null) : MergeClause
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("WHEN MATCHED");

            if(Predicate != null) 
            {
                writer.WriteSql($" AND {Predicate}");
            }

            writer.WriteSql($" THEN UPDATE SET {Assignments}");
        }
    }
    /// <summary>
    /// Matched delete clause
    /// </summary>
    /// <param name="Predicate">Delete expression</param>
    public record MatchedDelete(Expression? Predicate = null) : MergeClause
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("WHEN MATCHED");
        
            if (Predicate !=null)
            {
                writer.WriteSql($" AND {Predicate}");
            }

            writer.Write(" THEN DELETE");
        }
    }
    /// <summary>
    /// Not matched update clause
    /// </summary>
    /// <param name="Predicate">Expression predicate</param>
    /// <param name="Columns">Columns</param>
    /// <param name="Values">Values</param>
    public record NotMatched(Sequence<Ident> Columns, Values Values, Expression? Predicate = null) : MergeClause
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("WHEN NOT MATCHED");
            if (Predicate != null)
            {
                writer.WriteSql($" AND {Predicate}");
            }

            writer.WriteSql($" THEN INSERT ({Columns}) {Values}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}