namespace SqlParser.Ast;

/// <summary>
/// A table name or a parenthesized subquery with an optional alias
/// </summary>
public abstract record TableFactor : IWriteSql, IElement
{
    /// <summary>
    /// Common table alias across all implementations
    /// </summary>
    [Visit(1)] public TableAlias? Alias { get; set; }
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

    public record Function(bool Lateral, ObjectName Name, Sequence<FunctionArg> Args) : TableFactor
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Lateral)
            {
                writer.Write("LATERAL ");
            }

            writer.WriteSql($"{Name}({Args.ToSqlDelimited()})");

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
        [property: Visit(0)] TableFactor TableFactor,
        [property: Visit(2)] Sequence<ExpressionWithAlias> AggregateFunctions,
        Sequence<Ident> ValueColumns,
        PivotValueSource ValueSource,
        Expression? DefaultOnNull,
        TableAlias? PivotAlias = null
        ) : TableFactor
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var cols = new Expression.CompoundIdentifier(ValueColumns);

            writer.WriteSql($"{TableFactor} PIVOT({AggregateFunctions.ToSqlDelimited()} FOR {cols} IN ({ValueSource})");

            if (DefaultOnNull != null)
            {
                writer.WriteSql($" DEFAULT ON NULL ({DefaultOnNull})");
            }
            writer.Write(')');

            if (PivotAlias != null)
            {
                writer.WriteSql($" AS {PivotAlias}");
            }
        }
    }
    /// <summary>
    /// Unpivot table factor
    /// </summary>
    public record Unpivot(TableFactor TableFactor, Ident Value, Ident Name, Sequence<Ident> Columns) : TableFactor
    {
        public TableAlias? PivotAlias { get; set; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{TableFactor} UNPIVOT({Value} FOR {Name} IN ({Columns.ToSqlDelimited()}))");

            if (PivotAlias != null)
            {
                writer.WriteSql($" AS {PivotAlias}");
            }
        }
    }
    /// <summary>
    /// Table-based factor
    /// </summary>
    /// <param name="Name">Object name</param>
    public record Table(
        [property: Visit(0)] ObjectName Name,
        TableVersion? Version = null,
        Sequence<Ident>? Partitions = null)
        : TableFactor
    {
        /// Arguments of a table-valued function, as supported by Postgres
        /// and MSSQL. Note that deprecated MSSQL `FROM foo (NOLOCK)` syntax
        /// will also be parsed as `args`.
        ///
        /// This field's value is `Some(v)`, where `v` is a (possibly empty)
        /// vector of arguments, in the case of a table-valued function call,
        /// whereas it's `None` in the case of a regular table name.
        [Visit(2)] public TableFunctionArgs? Args { get; init; }
        /// MSSQL-specific `WITH (...)` hints such as NOLOCK.
        [Visit(3)] public Sequence<Expression>? WithHints { get; init; }

        /// Optional version qualifier to facilitate table time-travel, as supported by BigQuery and MSSQL.
        public TableVersion? Version { get; init; }
        public bool WithOrdinality { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name}");

            if (Args != null)
            {
                writer.Write("(");

                writer.WriteDelimited(Args.Arguments, Constants.SpacedComma);

                if (Args.Settings.SafeAny())
                {
                    if (Args.Arguments.SafeAny())
                    {
                        writer.WriteCommaSpaced();
                    }

                    writer.WriteSql($"SETTINGS {Args.Settings.ToSqlDelimited()}");
                }
                writer.Write(")");
            }

            if (WithOrdinality)
            {
                writer.Write(" WITH ORDINALITY");
            }

            if (Alias != null)
            {
                writer.WriteSql($" AS {Alias}");
            }

            if (WithHints.SafeAny())
            {
                writer.WriteSql($" WITH ({WithHints})");
            }

            if (Version != null)
            {
                writer.WriteSql($"{Version}");
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
        public bool WithOrdinality { get; set; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"UNNEST({ArrayExpressions.ToSqlDelimited()})");
         
            if (WithOrdinality)
            {
                writer.Write(" WITH ORDINALITY");
            }

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
    /// <summary>
    /// The `JSON_TABLE` table-valued function.
    /// Part of the SQL standard, but implemented only by MySQL, Oracle, and DB2.
    /// 
    /// SELECT * FROM JSON_TABLE(
    ///    '[{"a": 1, "b": 2}, {"a": 3, "b": 4}]',
    ///    '$[*]' COLUMNS(
    ///        a INT PATH '$.a' DEFAULT '0' ON EMPTY,
    ///        b INT PATH '$.b' NULL ON ERROR
    ///     )
    /// ) AS jt;
    /// </summary>
    /// <param name="JsonExpression"></param>
    /// <param name="JsonPath"></param>
    /// <param name="Columns"></param>
    public record JsonTable(Expression JsonExpression, Value JsonPath, Sequence<JsonTableColumn> Columns) : TableFactor
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"JSON_TABLE({JsonExpression}, {JsonPath} COLUMNS({Columns.ToSqlDelimited()}))");

            if (Alias != null)
            {
                writer.WriteSql($" AS {Alias}");
            }
        }
    }
    /// <summary>
    /// Match_Recognize operation on a table
    /// </summary>
    /// <param name="MatchTable">Table factor</param>
    /// <param name="PartitionBy">Partition By expression</param>
    /// <param name="OrderBy">Order By expression</param>
    /// <param name="Measures">Measures expression</param>
    /// <param name="RowsPerMatch"></param>
    /// <param name="AfterMatchSkip">ONE ROW PER MATCH | ALL ROWS PER MATCH</param>
    /// <param name="Pattern">Pattern</param>
    /// <param name="Symbols">Define Symbols</param>
    /// <param name="Alias">Define Alias</param>
    public record MatchRecognize(
        TableFactor MatchTable,
        Sequence<Expression> PartitionBy,
        Sequence<OrderByExpression> OrderBy,
        Sequence<Measure> Measures,
        RowsPerMatch? RowsPerMatch,
        AfterMatchSkip? AfterMatchSkip,
        MatchRecognizePattern Pattern,
        Sequence<SymbolDefinition> Symbols,
        TableAlias? Alias
    ) : TableFactor
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{MatchTable} MATCH_RECOGNIZE(");

            if (PartitionBy.SafeAny())
            {
                writer.Write($"PARTITION BY {PartitionBy.ToSqlDelimited(SqlParser.Symbols.Comma)} ");
            }

            if (OrderBy.SafeAny())
            {
                writer.Write($"ORDER BY {OrderBy.ToSqlDelimited()} ");
            }

            if (Measures.SafeAny())
            {
                writer.Write($"MEASURES {Measures.ToSqlDelimited()} ");
            }

            if (RowsPerMatch != null)
            {
                writer.WriteSql($"{RowsPerMatch} ");
            }

            if (AfterMatchSkip != null)
            {
                writer.WriteSql($"{AfterMatchSkip} ");
            }

            writer.WriteSql($"PATTERN ({Pattern}) DEFINE {Symbols.ToSqlDelimited()})");

            if (Alias != null)
            {
                writer.WriteSql($" AS {Alias}");
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