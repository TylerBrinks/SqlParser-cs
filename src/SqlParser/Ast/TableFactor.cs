namespace SqlParser.Ast;

/// <summary>
/// A table name or a parenthesized subquery with an optional alias
/// </summary>
public abstract record TableFactor : IWriteSql, IElement
{
    /// <summary>
    /// Common table alias across all implementations
    /// </summary>
    [Visit(1)] public TableAlias? Alias { get; internal set; }
    /// <summary>
    /// Derived table factor
    /// </summary>
    /// <param name="SubQuery">Subquery</param>
    /// <param name="Lateral">True if lateral</param>
    public record Derived(Query SubQuery, bool Lateral = false) : TableFactor
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Lateral)
            {
                writer.Write("LATERAL ");
            }

            writer.WriteSql($"({SubQuery})");

            if (Alias != null)
            {
                writer.WriteSql($" AS {Alias}");
            }
        }
    }
    /// <summary>
    /// Represents a parenthesized table factor. The SQL spec only allows a
    /// join expression ((foo JOIN bar [ JOIN baz ... ])) to be nested,
    /// possibly several times.
    ///
    /// The parser may also accept non-standard nesting of bare tables for some
    /// dialects, but the information about such nesting is stripped from AST.
    /// </summary>
    public record NestedJoin : TableFactor
    {
        public TableWithJoins? TableWithJoins { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"({TableWithJoins})");

            if (Alias != null)
            {
                writer.WriteSql($" AS {Alias}");
            }
        }
    }
    /// <summary>
    /// Pivot table factor
    /// </summary>
    public record Pivot(
        [property: Visit(0)] ObjectName Name,
        [property: Visit(2)] Expression AggregateFunction,
        Sequence<Ident> ValueColumns,
        Sequence<Value> PivotValues) : TableFactor
    {
        public TableAlias? PivotAlias { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name}");

            if (Alias != null)
            {
                writer.WriteSql($" AS {Alias}");
            }

            var cols = new Expression.CompoundIdentifier(ValueColumns);
            writer.WriteSql($" PIVOT({AggregateFunction} FOR {cols} IN (");

            writer.WriteSql($"{PivotValues}");

            writer.Write("))");

            if (PivotAlias != null)
            {
                writer.WriteSql($" AS {PivotAlias}");
            }
        }
    }
    /// <summary>
    /// Table0based factor
    /// </summary>
    /// <param name="Name">Object name</param>
    public record Table([property: Visit(0)] ObjectName Name) : TableFactor
    {
        /// Arguments of a table-valued function, as supported by Postgres
        /// and MSSQL. Note that deprecated MSSQL `FROM foo (NOLOCK)` syntax
        /// will also be parsed as `args`.
        ///
        /// This field's value is `Some(v)`, where `v` is a (possibly empty)
        /// vector of arguments, in the case of a table-valued function call,
        /// whereas it's `None` in the case of a regular table name.
        [Visit(2)] public Sequence<FunctionArg>? Args { get; init; }
        /// MSSQL-specific `WITH (...)` hints such as NOLOCK.
        [Visit(3)] public Sequence<Expression>? WithHints { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name}");

            if (Args != null)
            {
                writer.WriteSql($"({Args})");
            }

            if (Alias != null)
            {
                writer.WriteSql($" AS {Alias}");
            }

            if (WithHints.SafeAny())
            {
                writer.WriteSql($" WITH ({WithHints})");
            }
        }
    }
    /// <summary>
    /// Table function
    /// <example>
    /// <c>
    /// TABLE(expr)[ AS alias ]
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Function expression</param>
    public record TableFunction(Expression Expression) : TableFactor
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"TABLE({Expression})");

            if (Alias != null)
            {
                writer.WriteSql($" AS {Alias}");
            }
        }
    }
    /// <summary>
    /// Unnest expression
    ///
    /// <example>
    /// SELECT * FROM UNNEST ([10,20,30]) as numbers WITH OFFSET;
    /// +---------+--------+
    /// | numbers | offset |
    /// +---------+--------+
    /// | 10      | 0      |
    /// | 20      | 1      |
    /// | 30      | 2      |
    /// +---------+--------+
    /// </example>
    /// </summary>
    public record UnNest(Sequence<Expression> ArrayExpressions) : TableFactor
    {
        public bool WithOffset { get; init; }
        public Ident? WithOffsetAlias { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("UNNEST(");
            writer.WriteDelimited(ArrayExpressions, ", ");
            writer.Write(")");

            if (Alias != null)
            {
                writer.WriteSql($" AS {Alias}");
            }

            if (WithOffset)
            {
                writer.Write(" WITH OFFSET");
            }

            if (WithOffsetAlias != null)
            {
                writer.WriteSql($" AS {WithOffsetAlias}");
            }
        }
    }

    public T As<T>() where T : TableFactor
    {
        return (T)this;
    }

    public Table AsTable()
    {
        return As<Table>();
    }

    public abstract void ToSql(SqlTextWriter writer);
}