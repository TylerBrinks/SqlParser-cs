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
    /// ALL operation e.g. `1 ALL (1)` or `foo > ALL(bar)`, It will be wrapped on the right side of BinaryExpr
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
    /// Any operation e.g. `1 ANY (1)` or `foo > ANY(bar)`, It will be wrapped on the right side of BinaryExpr
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
    /// AT a timestamp to a different timezone
    /// <example>
    /// <c>
    /// FROM_UNIXTIME(0) AT TIME ZONE 'UTC-06:00'
    /// </c>
    /// </example>
    /// </summary>
    public record AtTimeZone(Expression Timestamp, Expression TimeZone) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Timestamp} AT TIME ZONE {TimeZone}");
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
                if (CustomOperator != null)
                {
                    writer.WriteSql($"{Left} {CustomOperator} {Right}");
                }
                else
                {
                    writer.WriteSql($"{Left} {Op} {Right}");
                }
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
                    writer.Write(')');
                }

                writer.WriteSql($" {Right}");
            }
        }

        public Sequence<string?>? PgOptions { get; init; }
        internal string? CustomOperator { get; private set; }

        public void SetCustomOperator(string customOperator)
        {
            CustomOperator = customOperator;
        }

        public virtual bool Equals(BinaryOp? other)
        {
            if (ReferenceEquals(null, other)){ return false; }
            if (ReferenceEquals(this, other)){ return true; }

            return base.Equals(other) &&
                   Equals(PgOptions, other.PgOptions) && 
                   Left.Equals(other.Left) && 
                   Op == other.Op && 
                   Right.Equals(other.Right);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(base.GetHashCode(), PgOptions, Left, (int)Op, Right);
        }
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
    public record Cast(Expression Expression, DataType DataType, CastKind Kind, CastFormat? Format = null) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Kind == CastKind.DoubleColon)
            {
                writer.WriteSql($"{Expression}::{DataType}");
                return;
            }

            var kind = Kind switch
            {
                CastKind.Cast => "CAST",
                CastKind.TryCast => "TRY_CAST",
                CastKind.SafeCast => "SAFE_CAST"
            };
          
            if (Format != null)
            {
                writer.WriteSql($"{kind}({Expression} as {DataType} FORMAT {Format})");
            }
            else
            {
                writer.WriteSql($"{kind}({Expression} as {DataType})");
            }
        }
    }
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
            if (Field is DateTimeField.NoDateTime)
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
    /// CONVERT a value to a different data type or character encoding `CONVERT(foo USING utf8mb4)`
    /// </summary>
    public record Convert(
        Expression Expression,
        DataType? DataType, 
        ObjectName? CharacterSet, 
        bool TargetBeforeValue,
        Sequence<Expression> Styles) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("CONVERT(");

            if (DataType != null)
            {
                if (CharacterSet != null)
                {
                    writer.WriteSql($"{Expression}, {DataType} CHARACTER SET {CharacterSet}");
                }
                else if (TargetBeforeValue)
                {
                    writer.WriteSql($"{DataType}, {Expression}");
                }
                else
                {
                    writer.WriteSql($"{Expression}, {DataType}");
                }
            }
            else if (CharacterSet != null)
            {
                writer.WriteSql($"{Expression} USING {CharacterSet}");
            }
            else
            {
                writer.WriteSql($"{Expression}");
            }

            if (Styles.SafeAny())
            {
                writer.WriteSql($", {Styles.ToSqlDelimited()}");
            }

            writer.Write(')');
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
                    writer.WriteSpacesComma();
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

            writer.Write(')');
        }
    }
    /// <summary>
    /// Dictionary expression
    /// </summary>
    /// <param name="Fields"></param>
    public record Dictionary(Sequence<DictionaryField> Fields) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"{{{Fields.ToSqlDelimited()}}}");
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
            if (Field is DateTimeField.NoDateTime)
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
        public FunctionArguments Args { get; internal set; }

        public FunctionArguments Parameters { get; init; }

        /// <summary>
        /// e.g. `x > 5` in `COUNT(x) FILTER (WHERE x > 5)`
        /// </summary>
        public Expression? Filter { get; init; }

        /// <summary>
        /// e.g. `x > 5` in `COUNT(x) FILTER (WHERE x > 5)`
        /// </summary>
        public NullTreatment? NullTreatment { get; init; }

        /// <summary>
        /// Window spec
        /// </summary>
        public WindowType? Over { get; init; }

        /// <summary>
        /// WITHIN GROUP (ORDER BY key [ASC | DESC], ...)
        /// </summary>
        public Sequence<OrderByExpression>? WithinGroup { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {

            writer.WriteSql($"{Name}{Parameters}{Args}");

            if (WithinGroup.SafeAny())
            {
                writer.WriteSql($" WITHIN GROUP (ORDER BY {WithinGroup!.ToSqlDelimited()})");
            }

            if (Filter != null)
            {
                writer.WriteSql($" FILTER (WHERE {Filter})");
            }

            if (NullTreatment != null)
            {
                writer.WriteSql($" {NullTreatment}");
            }

            if (Over != null)
            {
                writer.WriteSql($" OVER {Over}");

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
                    writer.WriteSpacesComma();
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
    public record ILike(Expression Expression, bool Negated, Expression Pattern, string? EscapeChar = null) : Expression, INegated
    {
        public ILike(Expression expression, bool negated, Expression pattern, char? escapeChar = null)
            : this(expression, negated, pattern, escapeChar?.ToString()) { }

        public ILike(Expression? expression, bool negated, Expression pattern)
            : this(expression, negated, pattern, (string?)null) { }
        
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
    /// <example>
    /// <c>
    /// INTERVAL '123:45.67' MINUTE(3) TO SECOND(2)
    /// </c>
    /// </example>.
    /// </summary>
    public record Interval : Expression
    {
        /// <param name="value">Value</param>
        /// <param name="leadingField">Date time leading field</param>
        /// <param name="lastField">Date time last field</param>
        public Interval(Expression value, DateTimeField? leadingField = null, DateTimeField? lastField = null)
        {
            Value = value;
            LeadingField = leadingField ?? new DateTimeField.None();
            LastField = lastField ?? new DateTimeField.None();
        }
        
        public Expression Value { get; }
        public DateTimeField LeadingField { get; }
        public DateTimeField LastField { get; }

        public ulong? LeadingPrecision { get; init; }
        /// The seconds precision can be specified in SQL source as
        /// `INTERVAL '__' SECOND(_, x)` (in which case the `leading_field`
        /// will be `Second` and the `last_field` will be `None`),
        /// or as `__ TO SECOND(x)`.
        public ulong? FractionalSecondsPrecision { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            // Length the leading field is SECOND, the parser guarantees that the last field is None.
            if (LeadingField is DateTimeField.Second && LeadingPrecision != null)
            {
                writer.WriteSql($"INTERVAL {Value} SECOND ({LeadingPrecision}, {FractionalSecondsPrecision})");
            }
            else
            {
                writer.WriteSql($"INTERVAL {Value}");

                if (LeadingField is not DateTimeField.None)
                {
                    writer.WriteSql($" {LeadingField}");
                }

                if (LeadingPrecision != null)
                {
                    writer.WriteSql($" ({LeadingPrecision})");
                }

                if (LastField is not DateTimeField.None)
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
    /// <param name="Value">Value expression</param>
    /// <param name="Path">Json path</param>
    public record JsonAccess(Expression Value, JsonPath Path) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Value}{Path}");
        }
    }
    /// <summary>
    /// Lambda function
    /// See https://docs.databricks.com/en/sql/language-manual/sql-ref-lambda-functions.html
    /// </summary>
    public record Lambda(LambdaFunction LambdaFunction) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{LambdaFunction}");
        }
    }
    /// <summary>
    /// LIKE expression
    /// </summary>
    /// <param name="Expression">Expression</param>
    /// <param name="Negated">Negated</param>
    /// <param name="Pattern">pattern expression</param>
    /// <param name="EscapeChar">Escape character</param>
    public record Like(Expression? Expression, bool Negated, Expression Pattern, string? EscapeChar = null) : Expression, INegated
    {
        public Like(Expression? expression, bool negated, Expression pattern, char? escapeChar = null)
            : this(expression, negated, pattern, escapeChar?.ToString()) { }

        public Like(Expression? expression, bool negated, Expression pattern)
            : this(expression, negated, pattern, (string?)null) { }

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

    public record Map(Ast.Map MapExpression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
           writer.WriteSql($"{MapExpression}");
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
    /// column['field'] or column
    /// </c>
    /// </example>
    public record MapAccess(Expression Column, Sequence<MapAccessKey> Keys) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Column}{Keys.ToSqlDelimited(string.Empty)}");
        }
    }
    /// <summary>
    /// Map access key
    /// </summary>
    /// <param name="Key">Key</param>
    /// <param name="Syntax">Syntax</param>
    public record MapAccessKey(Expression Key, MapAccessSyntax Syntax) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Syntax == MapAccessSyntax.Bracket)
            {
                writer.WriteSql($"[{Key}]");
            }
            else
            {
                writer.WriteSql($".{Key}");
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
    /// BigQuery specific: A named expression in a typeless struct
    /// </summary>
    public record Named(Expression Expression, Ident Name) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} AS {Name}");
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
    /// Some dialects support an older syntax for outer joins where columns are
    /// marked with the `(+)` operator in the WHERE clause, for example:
    ///
    /// SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2 (+)
    ///
    /// which is equivalent to
    ///
    /// SELECT t1.c1, t2.c2 FROM t1 LEFT OUTER JOIN t2 ON t1.c1 = t2.c2
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record OuterJoin(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} (+)");
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
    /// A reference to the prior level in a CONNECT BY clause.
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record Prior(Expression Expression) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"PRIOR {Expression}");
        }
    }
    /// <summary>
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    /// (Same caveats apply to `QualifiedWildcard` as to `Wildcard`.)
    /// </summary>
    /// <param name="Name">Object name</param>
    public record QualifiedWildcard(ObjectName Name) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name}.*");
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
                    writer.WriteSpacesComma();
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

            writer.Write(')');
        }
    }
    /// <summary>
    /// SimilarTo regex
    /// </summary>
    public record SimilarTo(Expression Expression, bool Negated, Expression Pattern, string? EscapeChar = null) : Expression, INegated
    {
        public SimilarTo(Expression expression, bool negated, Expression pattern, char? escapeChar = null)
            : this(expression, negated, pattern, escapeChar?.ToString()){ }

        public SimilarTo(Expression expression, bool negated, Expression pattern)
            : this(expression, negated, pattern, (string?) null) { }

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
    /// BigQuery specific Struct literal expression
    /// </summary>
    public record Struct(Sequence<Expression> Values, Sequence<StructField> Fields) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(Fields.SafeAny()
                ? $"STRUCT<{Fields.ToSqlDelimited()}>({Values.ToSqlDelimited()})"
                : $"STRUCT({Values.ToSqlDelimited()})");
        }
    }

    public record Subscript(Expression Expression, Ast.Subscript Key) : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression}[{Key}]");
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
                writer.Write($", {TrimCharacters.ToSqlDelimited()}");
            }

            writer.Write(')');
        }
    }
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
    /// <summary>
    /// Wildcard expression
    /// </summary>
    public record Wildcard : Expression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write('*');
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

public record ExpressionWithAlias(Expression Expression, Ident? Alias) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Expression}");

        if (Alias != null)
        {
            writer.WriteSql($" AS {Alias}");
        }
    }
}