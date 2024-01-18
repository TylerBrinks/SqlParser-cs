namespace SqlParser.Ast;

// ReSharper disable CommentTypo

public abstract record Expression : IWriteSql, IElement
{
    public interface INegated
    {
        bool Negated { get; init; }

        string? NegatedText => Negated ? "NOT " : null;
    }

    /// <summary>
    /// Case-based expression and data type
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="DataType">Data type</param>
    public abstract record CastBase(Expression Expression, DataType DataType, CastFormat? Format) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var cast = this switch
            {
                Cast => "CAST",
                SafeCast => "SAFE_CAST",
                TryCast => "TRY_CAST",
                _ => string.Empty
            };

            writer.WriteSql($"{cast}({Expression} AS {DataType}");

            if (Format != null)
            {
                writer.WriteSql($" FORMAT {Format}");
            }

            writer.Write(")");
        }
    }
    /// <summary>
    /// Aggregate function with filter
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="Filter">Filter</param>
    public record AggregateExpressionWithFilter(Expression Expression, Expression Filter) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} FILTER (WHERE {Filter})");
        }
    }
    /// <summary>
    /// ALL operation e.g. `1 ALL (1)` or `foo > ALL(bar)`, It will be wrapped in the right side of BinaryExpr
    /// </summary>
    /// <param name="Left">Expression</param>
    /// <param name="CompareOp">Operator</param>
    /// <param name="Right">Expression</param>
    public record AllOp(Expression Left, BinaryOperator CompareOp, Expression Right) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Left} {CompareOp} ALL({Right})");
        }
    }
    /// <summary>
    /// Any operation e.g. `1 ANY (1)` or `foo > ANY(bar)`, It will be wrapped in the right side of BinaryExpr
    /// </summary>
    /// <param name="Left">Expression</param>
    /// <param name="CompareOp">Operator</param>
    /// <param name="Right">Expression</param>
    public record AnyOp(Expression Left, BinaryOperator CompareOp, Expression Right) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Left} {CompareOp} ANY({Right})");
        }
    }
    /// <summary>
    /// An array expression e.g. ARRAY[1, 2]
    /// </summary>
    /// <param name="Arr"></param>
    public record Array(ArrayExpression Arr) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Arr}");
        }
    }
    /// <summary>
    /// ARRAY_AGG function
    /// <example>
    /// <c>
    /// SELECT ARRAY_AGG(... ORDER BY ...)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="ArrayAggregate">Array aggregation</param>
    public record ArrayAgg(ArrayAggregate ArrayAggregate) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{ArrayAggregate}");
        }
    }
    /// <summary>
    /// An array index expression
    /// <example>
    /// <c>
    /// (ARRAY[1, 2])[1] or (current_schemas(FALSE))[1]
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Obj"></param>
    /// <param name="Indexes"></param>
    public record ArrayIndex(Expression Obj, Sequence<Expression> Indexes) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Obj}");

            foreach (var index in Indexes)
            {
                writer.WriteSql($"[{index}]");
            }
        }
    }
    /// <summary>
    /// An array subquery constructor
    /// <example>
    /// <c>
    /// SELECT ARRAY(SELECT 1 UNION SELECT 2)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Query">Subquery</param>
    public record ArraySubquery(Query Query) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ARRAY({Query})");
        }
    }
    /// <summary>
    /// AT a timestamp to a different timezone
    /// <example>
    /// <c>
    /// FROM_UNIXTIME(0) AT TIME ZONE 'UTC-06:00'
    /// </c>
    /// </example>
    /// </summary>
    public record AtTimeZone(Expression Timestamp, string? TimeZone) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Timestamp} AT TIME ZONE '{TimeZone}'");
        }
    }
    /// <summary>
    /// Between expression
    /// <example>
    /// <c>
    /// Expression [ NOT ] BETWEEN low> AND high
    /// </c>
    /// </example>
    /// </summary>
    public record Between(Expression Expression, bool Negated, Expression Low, Expression High) : Expression, INegated
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} {AsNegated.NegatedText}BETWEEN {Low} AND {High}");
        }
    }
    /// <summary>
    /// Binary operation
    /// <example>
    /// <c>
    /// 1 + 1 or foo > bar
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Left">Operation left hand expression</param>
    /// <param name="Op">Binary operator</param>
    /// <param name="Right">Operation right hand expression</param>
    public record BinaryOp(Expression Left, BinaryOperator Op, Expression Right) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Op is not BinaryOperator.PGCustomBinaryOperator)
            {
                writer.WriteSql($"{Left} {Op} {Right}");
            }
            else
            {
                writer.WriteSql($"{Left}");

                if (PgOptions != null)
                {
                    writer.Write(" OPERATOR(");

                    for (var i = 0; i < PgOptions?.Count; i++)
                    {
                        if (i > 0)
                        {
                            writer.Write(Symbols.Dot);
                        }

                        writer.Write(PgOptions[i]);
                    }
                    writer.Write(")");
                }

                writer.WriteSql($" {Right}");
            }
        }

        public Sequence<string?>? PgOptions { get; init; }
    }
    /// <summary>
    /// `CASE [operand] WHEN condition THEN result ... [ELSE result] END`
    ///
    /// Note we only recognize a complete single expression as `condition`,
    /// not ` 0` nor `1, 2, 3` as allowed in a `simple_when_clause` per
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause"/>
    /// </summary>
    /// <param name="Results">Case results</param>
    public record Case(Sequence<Expression> Conditions, Sequence<Expression> Results) : Expression
    {
        public Expression? Operand { get; init; }
        //public Sequence<Increment>? Conditions { get; init; }
        public Expression? ElseResult { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("CASE");

            if (Operand != null)
            {
                writer.WriteSql($" {Operand}");
            }

            if (Conditions.SafeAny())
            {
                for (var i = 0; i < Conditions.Count; i++)
                {
                    writer.WriteSql($" WHEN {Conditions[i]} THEN {Results[i]}");
                }
            }

            if (ElseResult != null)
            {
                writer.WriteSql($" ELSE {ElseResult}");
            }

            writer.Write(" END");
        }
    }
    /// <summary>
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    /// </summary>
    public record Cast(Expression Expression, DataType DataType, CastFormat? Format = null) : CastBase(Expression, DataType, Format);
    /// <summary>
    /// CEIL(Expression [TO DateTimeField])
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="Field">Date time field</param>
    public record Ceil(Expression Expression, DateTimeField Field) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (Field == DateTimeField.NoDateTime)
            {
                writer.WriteSql($"CEIL({Expression})");
            }
            else
            {
                writer.WriteSql($"CEIL({Expression} TO {Field})");
            }
        }
    }
    /// <summary>
    /// Collate expression
    /// <example>
    /// <c>
    /// Expression COLLATE collation
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="Collation">Collation</param>
    public record Collate(Expression Expression, ObjectName Collation) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} COLLATE {Collation}");
        }
    }
    /// <summary>
    /// Multi-part identifier, e.g. 
    /// <example>
    /// <c>
    /// table_alias.column or schema.table.col
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Idents">Name identifiers</param>
    public record CompoundIdentifier(Sequence<Ident> Idents) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteDelimited(Idents, ".");
        }
    }
    /// <summary>
    /// Composite Access Postgres
    /// <example>
    /// <c>
    /// SELECT (information_schema._pg_expandarray(array['i','i'])).n
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="Key">Key identifier</param>
    public record CompositeAccess(Expression Expression, Ident Key) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression}.{Key}");
        }
    }
    /// <summary>
    /// CUBE expresion.
    /// </summary>
    /// <param name="Sets">Sets</param>
    public record Cube(Sequence<Sequence<Expression>> Sets) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("CUBE (");

            for (var i = 0; i < Sets.Count; i++)
            {
                if (i > 0)
                {
                    writer.Write(", ");
                }

                // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                if (Sets[i].Count == 1)
                {
                    writer.WriteSql($"{Sets[i]}");
                }
                else
                {
                    writer.WriteSql($"({Sets[i]})");
                }
            }

            writer.Write(")");
        }
    }
    /// <summary>
    /// CAST an expression to a different data type
    /// <example>
    /// <c>
    /// CAST(foo AS VARCHAR(123))
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="SubQuery">Subquery</param>
    /// <param name="Negated">Exists negated</param>
    public record Exists(Query SubQuery, bool Negated = false) : Expression, INegated
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{AsNegated.NegatedText}EXISTS ({SubQuery})");
        }
    }
    /// <summary>
    /// <example>
    /// <c>
    /// EXTRACT(DateTimeField FROM expr)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="Field">Date time field</param>
    public record Extract(Expression Expression, DateTimeField Field) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"EXTRACT({Field} FROM {Expression})");
        }
    }
    /// <summary>
    /// Floor expression
    /// <example>
    /// <c>
    /// FLOOR(Expression [TO DateTimeField])
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="Field">Date time field</param>
    public record Floor(Expression Expression, DateTimeField Field) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (Field == DateTimeField.NoDateTime)
            {
                writer.WriteSql($"FLOOR({Expression})");
            }
            else
            {
                writer.WriteSql($"FLOOR({Expression} TO {Field})");
            }
        }
    }
    /// <summary>
    /// Scalar function call
    /// <example>
    /// <c>
    /// LEFT(foo, 5)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name">Function object name</param>
    public record Function(ObjectName Name) : Expression
    {
        /// <summary>
        /// Sequence function call
        /// </summary>
        public Sequence<FunctionArg>? Args { get; internal set; }
        /// <summary>
        /// Window spec
        /// </summary>
        public WindowType? Over { get; init; }
        /// <summary>
        /// Aggregate functions may specify eg `COUNT(DISTINCT x)`
        /// </summary>
        public bool Distinct { get; init; }
        /// <summary>
        /// Some functions must be called without trailing parentheses, for example Postgres\
        /// do it for current_catalog, current_schema, etc. This flags is used for formatting.
        /// </summary>
        public bool Special { get; init; }
        /// <summary>
        /// Required ordering for the function (if empty, there is no requirement).
        /// </summary>
        public Sequence<OrderByExpression>? OrderBy { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            if (Special)
            {
                Name.ToSql(writer);
            }
            else
            {
                var ordered = OrderBy is { Count: > 0 };

                var distinct = Distinct ? "DISTINCT " : null;
                writer.WriteSql($"{Name}({distinct}{Args}");

                if (ordered)
                {
                    var orderBy = ordered ? " ORDER BY " : string.Empty;
                    writer.Write(orderBy);
                    writer.WriteDelimited(OrderBy, ", ");
                }

                writer.Write(")");

                if (Over != null)
                {
                    writer.WriteSql($" OVER {Over}");
                }
            }
        }
    }
    /// <summary>
    /// GROUPING SETS expression.
    /// </summary>
    /// <param name="Expressions">Sets</param>
    public record GroupingSets(Sequence<Sequence<Expression>> Expressions) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("GROUPING SETS (");

            for (var i = 0; i < Expressions.Count; i++)
            {
                if (i > 0)
                {
                    writer.Write(", ");
                }

                writer.WriteSql($"({Expressions[i]})");
            }
            writer.Write(")");
        }
    }
    /// <summary>
    /// Identifier e.g. table name or column name
    /// </summary>
    /// <param name="Ident">Identifier name</param>
    public record Identifier(Ident Ident) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Ident.ToSql(writer);
        }
    }
    /// <summary>
    /// ILIKE (case-insensitive LIKE)
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public record ILike(Expression Expression, bool Negated, Expression Pattern, char? EscapeChar = null) : Expression, INegated
    {
        public override void ToSql(SqlTextWriter writer)
        {
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (EscapeChar != null)
            {
                writer.WriteSql($"{Expression} {AsNegated.NegatedText}ILIKE {Pattern} ESCAPE '{EscapeChar}'");
            }
            else
            {
                writer.WriteSql($"{Expression} {AsNegated.NegatedText}ILIKE {Pattern}");
            }
        }
    }
    /// <summary>
    /// In List expression
    /// <example>
    /// <c>
    /// [ NOT ] IN (val1, val2, ...)
    /// </c>
    /// </example>
    /// </summary>
    public record InList(Expression Expression, Sequence<Expression> List, bool Negated) : Expression, INegated
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} {AsNegated.NegatedText}IN ({List})");
        }
    }
    /// <summary>
    /// In Subqery expression
    /// <example>
    /// <c>
    ///`[ NOT ] IN (SELECT ...)
    /// </c>
    /// </example>
    /// </summary>
    public record InSubquery(Query SubQuery, bool Negated, Expression? Expression = null) : Expression, INegated
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} {AsNegated.NegatedText}IN ({SubQuery})");
        }
    }
    /// <summary>
    /// INTERVAL literals, roughly in the following format:
    ///
    /// INTERVAL 'value' [ leading_field [ (leading_precision) ] ]
    /// [ TO last_field [ (fractional_seconds_precision) ] ],
    ///
    /// The parser does not validate the `value`, nor does it ensure
    /// that the `leading_field` units >= the units in `last_field`,
    /// so the user will have to reject intervals like `HOUR TO YEAR`
    ///
    /// <example>
    /// <c>
    /// INTERVAL '123:45.67' MINUTE(3) TO SECOND(2)
    /// </c>
    /// </example>.
    /// </summary>
    /// <param name="Value">Value</param>
    /// <param name="LeadingField">Date time leading field</param>
    /// <param name="LastField">Date time last field</param>
    public record Interval(Expression Value, DateTimeField LeadingField = DateTimeField.None, DateTimeField LastField = DateTimeField.None) : Expression
    {
        public ulong? LeadingPrecision { get; init; }
        /// The seconds precision can be specified in SQL source as
        /// `INTERVAL '__' SECOND(_, x)` (in which case the `leading_field`
        /// will be `Second` and the `last_field` will be `None`),
        /// or as `__ TO SECOND(x)`.
        public ulong? FractionalSecondsPrecision { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            // Length the leading field is SECOND, the parser guarantees that the last field is None.
            if (LeadingField == DateTimeField.Second && LeadingPrecision != null)
            {
                writer.WriteSql($"INTERVAL {Value} SECOND ({LeadingPrecision}, {FractionalSecondsPrecision})");
            }
            else
            {
                writer.WriteSql($"INTERVAL {Value}");

                if (LeadingField != DateTimeField.None)
                {
                    writer.WriteSql($" {LeadingField}");
                }

                if (LeadingPrecision != null)
                {
                    writer.WriteSql($" ({LeadingPrecision})");
                }

                if (LastField != DateTimeField.None)
                {
                    writer.WriteSql($" TO {LastField}");
                }

                if (FractionalSecondsPrecision != null)
                {
                    writer.WriteSql($" ({FractionalSecondsPrecision})");
                }
            }
        }
    }
    /// <summary>
    /// Introduced string
    /// 
    /// <see href="https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html"/>
    /// </summary>
    public record IntroducedString(string Introducer, Value Value) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Introducer} {Value}");
        }
    }
    /// <summary>
    /// In Unnest expression
    /// <example>
    /// <c>
    /// [ NOT ] IN UNNEST(array_expression)
    /// </c>
    /// </example>
    /// </summary>
    public record InUnnest(Expression Expression, Expression ArrayExpression, bool Negated) : Expression, INegated
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} {AsNegated.NegatedText}IN UNNEST({ArrayExpression})");
        }
    }
    /// <summary>
    /// IS DISTINCT FROM operator
    /// </summary>
    /// <param name="Expression1">Expresison 1</param>
    /// <param name="Expression2">Expressoin 2</param>
    public record IsDistinctFrom(Expression Expression1, Expression Expression2) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression1} IS DISTINCT FROM {Expression2}");
        }
    }
    /// <summary>
    /// IS FALSE operator
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record IsFalse(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} IS FALSE");
        }
    }
    /// <summary>
    /// IS NOT DISTINCT FROM operator
    /// </summary>
    /// <param name="Expression1">Expresison 1</param>
    /// <param name="Expression2">Expressoin 1</param>
    public record IsNotDistinctFrom(Expression Expression1, Expression Expression2) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression1} IS NOT DISTINCT FROM {Expression2}");
        }
    }
    /// <summary>
    /// IS NOT FALSE operator
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record IsNotFalse(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} IS NOT FALSE");
        }
    }
    /// <summary>
    /// IS NOT NULL operator
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record IsNotNull(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} IS NOT NULL");
        }
    }
    /// <summary>
    /// IS NOT UNKNOWN operator
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record IsNotUnknown(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} IS NOT UNKNOWN");
        }
    }
    /// <summary>
    /// IS NULL operator
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record IsNull(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} IS NULL");
        }
    }
    /// <summary>
    /// IS NOT TRUE operator
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record IsNotTrue(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} IS NOT TRUE");
        }
    }
    /// <summary>
    /// IS TRUE operator
    /// </summary>
    public record IsTrue(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} IS TRUE");
        }
    }
    /// <summary>
    /// IS UNKNOWN operator
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record IsUnknown(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} IS UNKNOWN");
        }
    }
    /// <summary>
    /// JSON access (Postgres)  eg: data->'tags'
    /// </summary>
    /// <param name="Left">Left hand expression</param>
    /// <param name="Operator">Json Operator</param>
    /// <param name="Right">Right hand expression</param>
    public record JsonAccess(Expression Left, JsonOperator Operator, Expression Right) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (Operator == JsonOperator.Colon)
            {
                writer.WriteSql($"{Left}{Operator}{Right}");
            }
            else
            {
                writer.WriteSql($"{Left} {Operator} {Right}");
            }
        }
    }
    /// <summary>
    /// LIKE expression
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="Negated">Negated</param>
    /// <param name="Pattern">pattern expression</param>
    /// <param name="EscapeChar">Escape character</param>
    public record Like(Expression? Expression, bool Negated, Expression Pattern, char? EscapeChar = null) : Expression, INegated
    {
        public override void ToSql(SqlTextWriter writer)
        {
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (EscapeChar != null)
            {
                writer.WriteSql($"{Expression} {AsNegated.NegatedText}LIKE {Pattern} ESCAPE '{EscapeChar}'");
            }
            else
            {
                writer.WriteSql($"{Expression} {AsNegated.NegatedText}LIKE {Pattern}");
            }
        }
    }
    /// <summary>
    /// LISTAGG function
    /// <example>
    /// <c>
    /// SELECT LISTAGG(...) WITHIN GROUP (ORDER BY ...)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="ListAggregate">List aggregate</param>
    public record ListAgg(ListAggregate ListAggregate) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            ListAggregate.ToSql(writer);
        }
    }
    /// <summary>
    /// Literal value e.g. '5'
    /// </summary>
    /// <param name="Value">Value</param>
    public record LiteralValue(Value Value) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Value.ToSql(writer);
        }
    }
    /// Access a map-like object by field
    /// 
    /// Note that depending on the dialect, struct like accesses may be
    /// parsed as [ArrayIndex](Self::ArrayIndex) or `MapAcces`](Self::MapAccess)
    ///
    /// <see href="https://clickhouse.com/docs/en/sql-reference/data-types/map/"/>
    ///
    /// <example>
    /// <c>
    /// column['field'] or column[4]
    /// </c>
    /// </example>
    public record MapAccess(Expression Column, Sequence<Expression> Keys) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Column.ToSql(writer);

            foreach (var key in Keys)
            {
                // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                if (key is LiteralValue { Value: Value.SingleQuotedString s })
                {
                    writer.WriteSql($"[\"{s.Value}\"]");
                }
                else
                {
                    writer.WriteSql($"[{key}]");
                }
            }
        }
    }
    /// <summary>
    /// MySQL specific text search function
    ///
    /// <see href="https://dev.mysql.com/doc/refman/8.0/en/fulltext-search.html#function_match"/>
    ///
    /// <example>
    /// <c>
    /// MARCH (col, col, ...) AGAINST (Expression [search modifier])
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Columns">Columns</param>
    /// <param name="MatchValue">Match Value</param>
    /// <param name="OptSearchModifier">Search Modifier</param>
    public record MatchAgainst(Sequence<Ident> Columns, Value MatchValue, SearchModifier OptSearchModifier) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"MATCH ({Columns}) AGAINST ");

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (OptSearchModifier != SearchModifier.None)
            {
                writer.WriteSql($"({MatchValue} {OptSearchModifier})");
            }
            else
            {
                writer.WriteSql($"({MatchValue})");
            }
        }
    }
    /// <summary>
    /// Nested expression
    /// <example>
    /// <c>
    /// (foo > bar) or (1)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression"></param>
    public record Nested(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"({Expression})");
        }
    }
    /// <summary>
    /// Overlay expression
    /// <example>
    /// <c>
    /// OVERLAY(Expression PLACING Expression FROM expr[ FOR Expression ]
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="OverlayWhat">Overlay what expression</param>
    /// <param name="OverlayFrom">Overlay from expression</param>
    /// <param name="OverlayFor">Overlay for expression</param>
    public record Overlay(Expression Expression, Expression OverlayWhat, Expression OverlayFrom, Expression? OverlayFor = null) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"OVERLAY({Expression} PLACING {OverlayWhat} FROM {OverlayFrom}");

            if (OverlayFor != null)
            {
                writer.WriteSql($" FOR {OverlayFor}");
            }

            writer.WriteSql($")");
        }
    }
    /// <summary>
    /// Position expression
    /// <example>
    /// <c>
    /// (Expression in expr)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="In">In expression</param>
    public record Position(Expression Expression, Expression In) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"POSITION({Expression} IN {In})");
        }
    }
    /// <summary>
    /// MySql RLike regex or REGEXP regex
    /// </summary>
    /// <param name="Negated">True if nexted</param>
    /// <param name="Expression">Expression</param>
    /// <param name="Pattern">Expression pattern</param>
    /// <param name="RegularExpression">Regular expression</param>
    public record RLike(bool Negated, Expression Expression, Expression Pattern, bool RegularExpression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var negated = Negated ? "NOT " : null;
            var regexp = RegularExpression ? "REGEXP" : "RLIKE";

            writer.WriteSql($"{Expression} {negated}{regexp} {Pattern}");
        }
    }
    /// <summary>
    /// Rollup expression
    /// <example>
    /// <c>
    /// ROLLUP expr
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expressions">Sets</param>
    public record Rollup(Sequence<Sequence<Expression>> Expressions) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("ROLLUP (");

            for (var i = 0; i < Expressions.Count; i++)
            {
                if (i > 0)
                {
                    writer.Write(", ");
                }

                // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                if (Expressions[i].Count == 1)
                {
                    writer.WriteSql($"{Expressions[i]}");
                }
                else
                {
                    writer.WriteSql($"({Expressions[i]})");
                }
            }

            writer.Write(")");
        }
    }
    /// <summary>
    /// SAFE_CAST an expression to a different data type
    ///
    /// only available for BigQuery: <see href="https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#safe_casting"/>
    /// Works the same as `TRY_CAST`
    /// <example>
    /// <c>
    /// SAFE_CAST(foo AS FLOAT64)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="DataType"></param>
    public record SafeCast(Expression Expression, DataType DataType, CastFormat? Format = null) : CastBase(Expression, DataType, Format);
    /// <summary>
    /// SimilarTo regex
    /// </summary>
    public record SimilarTo(Expression Expression, bool Negated, Expression Pattern, char? EscapeChar = null) : Expression, INegated
    {
        public override void ToSql(SqlTextWriter writer)
        {
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (EscapeChar != null)
            {
                writer.WriteSql($"{Expression} {AsNegated.NegatedText}SIMILAR TO {Pattern} ESCAPE '{EscapeChar}'");
            }
            else
            {
                writer.WriteSql($"{Expression} {AsNegated.NegatedText}SIMILAR TO {Pattern}");
            }
        }
    }
    /// <summary>
    /// Substring expression
    /// <example>
    /// <c>
    /// SUBSTRING(Expression [FROM expr] [FOR expr])
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="SubstringFrom">From expression</param>
    /// <param name="SubstringFor">For expression</param>
    public record Substring(Expression Expression, Expression? SubstringFrom = null, Expression? SubstringFor = null, bool Special = false) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SUBSTRING({Expression}");

            if (SubstringFrom != null)
            {
                if (Special)
                {
                    writer.WriteSql($", {SubstringFrom}");
                }
                else
                {
                    writer.WriteSql($" FROM {SubstringFrom}");
                }
            }

            if (SubstringFor != null)
            {
                if (Special)
                {
                    writer.WriteSql($", {SubstringFor}");
                }
                else
                {
                    writer.WriteSql($" FOR {SubstringFor}");
                }
            }

            writer.WriteSql($")");
        }
    }
    /// <summary>
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// <example>
    /// <c>
    /// SELECT (subquery) AS x` or `WHERE (subquery) = x
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Query">Select</param>
    public record Subquery(Query Query) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"({Query})");
        }
    }
    /// <summary>
    /// Trim expression
    /// <example>
    /// <c>
    /// TRIM([BOTH | LEADING | TRAILING] [expr FROM] expr)
    ///
    /// TRIM(expr)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="TrimWhere">Trim where field</param>
    /// <param name="TrimWhat">What to trip expression</param>
    public record Trim(Expression Expression, TrimWhereField TrimWhere, Expression? TrimWhat = null, Sequence<Expression>? TrimCharacters = null) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TRIM(");

            if (TrimWhere != TrimWhereField.None)
            {
                writer.WriteSql($"{TrimWhere} ");
            }

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (TrimWhat != null)
            {
                writer.WriteSql($"{TrimWhat} FROM {Expression}");
            }
            else
            {
                writer.WriteSql($"{Expression}");
            }

            if (TrimCharacters.SafeAny())
            {
                writer.Write(", ");
                writer.WriteDelimited(TrimCharacters, ", ");
            }

            writer.Write(")");
        }
    }
    /// <summary>
    /// TRY_CAST an expression to a different data type
    ///
    /// this differs from CAST in the choice of how to implement invalid conversions
    /// <example>
    /// <c>
    /// TRY_CAST(foo AS VARCHAR(123))
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="DataType">Cast data type</param>
    public record TryCast(Expression Expression, DataType DataType, CastFormat? Format = null) : CastBase(Expression, DataType, Format);
    /// <summary>
    /// ROW / TUPLE a single value, such as `SELECT (1, 2)`
    /// </summary>
    /// <param name="Expressions">Sets</param>
    public record Tuple(Sequence<Expression> Expressions) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"({Expressions})");
        }
    }
    /// <summary>
    /// A constant of form data_type 'value'.
    /// This can represent ANSI SQL DATE, TIME, and TIMESTAMP literals (such as DATE '2020-01-01'),
    /// as well as constants of other types (a non-standard PostgreSQL extension).
    /// </summary>
    /// <param name="Value">Value</param>
    /// <param name="DataType">Optional data type</param>
    public record TypedString(string Value, DataType DataType) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            DataType.ToSql(writer);
            writer.Write($" '{Value.EscapeSingleQuoteString()}'");
        }
    }
    /// <summary>
    /// Unary operation e.g. `NOT foo`
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="Op"></param>
    public record UnaryOp(Expression Expression, UnaryOperator Op) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Op == UnaryOperator.PGPostfixFactorial)
            {
                writer.WriteSql($"{Expression}{Op}");
            }
            else if (Op == UnaryOperator.Not)
            {
                writer.WriteSql($"{Op} {Expression}");
            }
            else
            {
                writer.WriteSql($"{Op}{Expression}");
            }
        }
    }
    public virtual void ToSql(SqlTextWriter writer) { }

    internal INegated AsNegated => (INegated)this;

    public T As<T>() where T : Expression
    {
        return (T)this;
    }

    public BinaryOp AsBinaryOp()
    {
        return As<BinaryOp>();
    }

    public UnaryOp AsUnaryOp()
    {
        return As<UnaryOp>();
    }

    public Identifier AsIdentifier()
    {
        return As<Identifier>();
    }

    public LiteralValue AsLiteral()
    {
        return As<LiteralValue>();
    }
}
// ReSharper restore CommentTypo
