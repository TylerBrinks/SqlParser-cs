namespace SqlParser.Ast;

public record Join(TableFactor? Relation = null, JoinOperator? JoinOperator = null, bool Global = false) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        if (Global)
        {
            writer.Write(" GLOBAL");
        }

        switch (JoinOperator)
        {
            case JoinOperator.CrossApply:
                writer.WriteSql($" CROSS APPLY {Relation}");
                return;
            case JoinOperator.OuterApply:
                writer.WriteSql($" OUTER APPLY {Relation}");
                return;
            case JoinOperator.CrossJoin:
                writer.WriteSql($" CROSS JOIN {Relation}");
                return;

            case JoinOperator.AsOf a:
                writer.WriteSql($" ASOF JOIN {Relation} MATCH_CONDITION ({a.MatchCondition})");
                return;
        }

        string joinText = null!;
        JoinConstraint constraint = null!;
        switch (JoinOperator)
        {
            case JoinOperator.Inner inner:
                joinText = "JOIN";
                constraint = inner.JoinConstraint;
                break;
            case JoinOperator.LeftOuter left:
                joinText = "LEFT JOIN";
                constraint = left.JoinConstraint;
                break;
            case JoinOperator.RightOuter right:
                joinText = "RIGHT JOIN";
                constraint = right.JoinConstraint;
                break;
            case JoinOperator.FullOuter full:
                joinText = "FULL JOIN";
                constraint = full.JoinConstraint;
                break;
            case JoinOperator.LeftSemi leftSemi:
                joinText = "LEFT SEMI JOIN";
                constraint = leftSemi.JoinConstraint;
                break;
            case JoinOperator.RightSemi rightSemi:
                joinText = "RIGHT SEMI JOIN";
                constraint = rightSemi.JoinConstraint;
                break;
            case JoinOperator.LeftAnti leftAnti:
                joinText = "LEFT ANTI JOIN";
                constraint = leftAnti.JoinConstraint;
                break;
            case JoinOperator.RightAnti rightAnti:
                joinText = "RIGHT ANTI JOIN";
                constraint = rightAnti.JoinConstraint;
                break;
        }

        writer.WriteSql($" {Prefix(constraint)}{joinText} {Relation}{Suffix(constraint)}");
        return;

        string? Suffix(JoinConstraint constraint)
        {
            return constraint switch
            {
                JoinConstraint.On on => $" ON {on.Expression.ToSql()}",
                JoinConstraint.Using @using => $" USING({@using.Idents.ToSqlDelimited()})",
                _ => null
            };
        }

        string? Prefix(JoinConstraint prefixConstraint)
        {
            return prefixConstraint is JoinConstraint.Natural ? "NATURAL " : null;
        }
    }
}

/// <summary>
/// Join operator
/// </summary>
public abstract record JoinOperator : IElement
{
    public abstract record ConstrainedJoinOperator(JoinConstraint JoinConstraint) : JoinOperator;

    /// <summary>
    /// Inner join
    /// </summary>
    /// <param name="JoinConstraint">Join constraint</param>
    public record Inner(JoinConstraint JoinConstraint) : ConstrainedJoinOperator(JoinConstraint);
    /// <summary>
    /// Left join
    /// </summary>
    /// <param name="JoinConstraint">Join constraint</param>
    public record LeftOuter(JoinConstraint JoinConstraint) : ConstrainedJoinOperator(JoinConstraint);
    /// <summary>
    /// Right outer join
    /// </summary>
    /// <param name="JoinConstraint">Join constraint</param>
    public record RightOuter(JoinConstraint JoinConstraint) : ConstrainedJoinOperator(JoinConstraint);
    /// <summary>
    /// Full outer join
    /// </summary>
    /// <param name="JoinConstraint">Join constraint</param>
    public record FullOuter(JoinConstraint JoinConstraint) : ConstrainedJoinOperator(JoinConstraint);
    /// <summary>
    /// Cross join
    /// </summary>
    public record CrossJoin : JoinOperator;
    /// <summary>
    /// Left semi join
    /// </summary>
    /// <param name="JoinConstraint">Join constraint</param>
    public record LeftSemi(JoinConstraint JoinConstraint) : ConstrainedJoinOperator(JoinConstraint);
    /// <summary>
    /// Right semi join
    /// </summary>
    /// <param name="JoinConstraint">Join constraint</param>
    public record RightSemi(JoinConstraint JoinConstraint) : ConstrainedJoinOperator(JoinConstraint);
    /// <summary>
    /// Left anti join
    /// </summary>
    /// <param name="JoinConstraint">Join constraint</param>
    public record LeftAnti(JoinConstraint JoinConstraint) : ConstrainedJoinOperator(JoinConstraint);
    /// <summary>
    /// Right anti join
    /// </summary>
    /// <param name="JoinConstraint">Join constraint</param>
    public record RightAnti(JoinConstraint JoinConstraint) : ConstrainedJoinOperator(JoinConstraint);
    /// <summary>
    /// Cross apply join
    /// </summary>
    public record CrossApply : JoinOperator;
    /// <summary>
    /// Outer apply join
    /// </summary>
    public record OuterApply : JoinOperator;

    public record AsOf(Expression MatchCondition, JoinConstraint Constraint) : JoinOperator;
}

/// <summary>
/// Join constraint
/// </summary>
public abstract record JoinConstraint : IElement
{
    /// <summary>
    /// On join constraint
    /// </summary>
    /// <param name="Expression">Constraint expression</param>
    public record On(Expression Expression) : JoinConstraint;
    /// <summary>
    /// Using join constraint
    /// </summary>
    /// <param name="Idents">Name identifiers</param>
    public record Using(Sequence<Ident> Idents) : JoinConstraint;
    /// <summary>
    /// Natural join constraint
    /// </summary>
    public record Natural : JoinConstraint;
    /// <summary>
    /// No join constraint
    /// </summary>
    public record None : JoinConstraint;
}