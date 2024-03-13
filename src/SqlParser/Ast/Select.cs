namespace SqlParser.Ast;

/// <summary>
/// A restricted variant of SELECT (without CTEs/ORDER BY), which may
/// appear either as the only body item of a Select, or as an operand
/// to a set operation like UNION.
/// </summary>
/// <param name="Projection">Select projections</param>
public record Select([Visit(1)] Sequence<SelectItem> Projection) : IWriteSql, IElement
{
    public DistinctFilter? Distinct { get; init; }
    [Visit(0)] public Top? Top { get; init; }
    [Visit(2)] public SelectInto? Into { get; init; }
    [Visit(3)] public Sequence<TableWithJoins>? From { get; init; }
    [Visit(4)] public Sequence<LateralView>? LateralViews { get; init; }
    [Visit(5)] public Expression? Selection { get; init; }
    [Visit(6)] public GroupByExpression? GroupBy { get; init; }
    [Visit(7)] public Sequence<Expression>? ClusterBy { get; init; }
    [Visit(8)] public Sequence<Expression>? DistributeBy { get; init; }
    [Visit(9)] public Sequence<Expression>? SortBy { get; init; }
    [Visit(10)] public Expression? Having { get; init; }
    [Visit(11)] public Sequence<NamedWindowDefinition>? NamedWindow { get; init; }
    [Visit(12)] public Expression? QualifyBy { get; init; }
    [Visit(13)] public ValueTableMode? ValueTableMode { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("SELECT");

        if (ValueTableMode != null)
        {
            writer.WriteSql($" {ValueTableMode}");
        }

        if (Distinct != null)
        {
            writer.WriteSql($" {Distinct}");
        }

        if (Top != null)
        {
            writer.WriteSql($" {Top}");
        }

        writer.WriteSql($" {Projection}");

        if (Into != null)
        {
            writer.WriteSql($" {Into}");
        }

        if (From != null)
        {
            writer.WriteSql($" FROM {From}");
        }

        if (LateralViews.SafeAny())
        {
            foreach (var view in LateralViews!)
            {
               view.ToSql(writer);
            }
        }

        if (Selection != null)
        {
            writer.WriteSql($" WHERE {Selection}");
        }

        if (GroupBy != null)
        {
            writer.WriteSql($" {GroupBy}");
        }

        if (ClusterBy != null)
        {
            writer.WriteSql($" CLUSTER BY {ClusterBy}");
        }

        if (DistributeBy != null)
        {
            writer.WriteSql($" DISTRIBUTE BY {DistributeBy}");
        }

        if (SortBy != null)
        {
            writer.WriteSql($" SORT BY {SortBy}");
        }

        if (Having != null)
        {
            writer.WriteSql($" HAVING {Having}");
        }

        if (NamedWindow.SafeAny())
        {
            writer.Write(" WINDOW ");
            writer.WriteDelimited(NamedWindow, ", ");
        }

        if (QualifyBy != null)
        {
            writer.WriteSql($" QUALIFY {QualifyBy}");
        }
    }
}