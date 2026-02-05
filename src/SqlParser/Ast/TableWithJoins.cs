namespace SqlParser.Ast;

/// <summary>
/// Represents a table with joins and table relationships
/// </summary>
/// <param name="Relation">Relation table factor</param>
public record TableWithJoins(TableFactor Relation) : IWriteSql, IElement
{
    [Visit(0)] public TableFactor? Relation { get; set; } = Relation;

    [Visit(1)] public Sequence<Join>? Joins { get; set; }

    public void ToSql(SqlTextWriter writer)
    {
        Relation?.ToSql(writer);

        if (Joins.SafeAny())
        {
            JoinOperator? prevOp = null;
            foreach (var join in Joins!)
            {
                var isArrayJoin = join.JoinOperator is JoinOperator.InnerArrayJoin or JoinOperator.LeftArrayJoin;
                var sameArrayJoinType = isArrayJoin && prevOp != null && prevOp.GetType() == join.JoinOperator!.GetType();
                if (sameArrayJoinType)
                {
                    // Combine consecutive ARRAY JOINs as comma-separated items
                    writer.WriteSql($", {join.Relation}");
                }
                else
                {
                    join.ToSql(writer);
                }
                prevOp = join.JoinOperator;
            }
        }
    }
}