namespace SqlParser.Ast;

/// <summary>
/// A node in a tree, representing a "query body" expression, roughly:
/// SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]
/// </summary>
public abstract record SetExpression : IWriteSql, IElement
{
    /// <summary>
    /// Insert query bdy
    /// </summary>
    /// <param name="Statement">Statement</param>
    public record Insert(Statement Statement) : SetExpression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Statement.ToSql(writer);
        }
    }
    /// <summary>
    /// Select expression body
    /// </summary>
    /// <param name="Query"></param>
    public record QueryExpression(Query Query) : SetExpression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"({Query})");
        }
    }

    //public record QueryExpression([Visit(1)] SetExpression Body) : IWriteSql, IElement
    //{
    //    [Visit(0)] public With? With { get; init; }
    //    [Visit(2)] public Sequence<OrderByExpression>? OrderBy { get; set; }
    //    [Visit(3)] public Expression? Limit { get; init; }
    //    [Visit(4)] public Offset? Offset { get; init; }
    //    [Visit(5)] public Fetch? Fetch { get; init; }
    //    [Visit(6)] public Sequence<LockClause>? Locks { get; init; }

    //    //public static implicit operator Query(Statement.Select select)
    //    //{
    //    //    return select.Query;
    //    //}

    //    //public static implicit operator Statement.Select(Query query)
    //    //{
    //    //    return new Statement.Select(query);
    //    //}

    //    public void ToSql(SqlTextWriter writer)
    //    {
    //        if (With != null)
    //        {
    //            writer.WriteSql($"{With} ");
    //        }

    //        Body.ToSql(writer);

    //        if (OrderBy != null)
    //        {
    //            writer.WriteSql($" ORDER BY {OrderBy}");
    //        }

    //        if (Limit != null)
    //        {
    //            writer.WriteSql($" LIMIT {Limit}");
    //        }

    //        if (Offset != null)
    //        {
    //            writer.WriteSql($" {Offset}");
    //        }

    //        if (Fetch != null)
    //        {
    //            writer.WriteSql($" {Fetch}");
    //        }

    //        if (Locks != null && Locks.Any())
    //        {
    //            writer.WriteSql($" {Locks.ToSqlDelimited(Symbols.Space.ToString())}");
    //        }
    //    }
    //}

    /// <summary>
    /// Select expression body
    /// </summary>
    /// <param name="Select"></param>
    public record SelectExpression(Select Select) : SetExpression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Select.ToSql(writer);
        }
    }
    /// <summary>
    /// Set operation body
    /// </summary>
    /// <param name="Left">Left hand expression</param>
    /// <param name="Op">Set operator</param>
    /// <param name="Right">Right hand expression</param>
    /// <param name="SetQuantifier">Set quantifier</param>
    public record SetOperation(SetExpression Left, SetOperator Op, SetExpression Right, SetQuantifier SetQuantifier) : SetExpression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Left} {Op}");

            if (SetQuantifier != SetQuantifier.None)
            {
                writer.WriteSql($" {SetQuantifier}");
            }

            writer.WriteSql($" {Right}");
        }
    }
    /// <summary>
    /// Table expression
    /// </summary>
    /// <param name="Table">Table</param>
    public record TableExpression(Table Table) : SetExpression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Table.ToSql(writer);
        }
    }
    /// <summary>
    /// Values expression
    /// </summary>
    /// <param name="Values">Values</param>
    public record ValuesExpression(Values Values) : SetExpression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Values.ToSql(writer);
        }
    }

    public abstract void ToSql(SqlTextWriter writer);

    public T As<T>() where T : SetExpression
    {
        return (T)this;
    }

    public SelectExpression AsSelectExpression()
    {
        return As<SelectExpression>();
    }

    public Select AsSelect()
    {
        return AsSelectExpression().Select;
    }
}