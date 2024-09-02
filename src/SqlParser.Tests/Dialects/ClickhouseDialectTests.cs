using FluentAssertions.Formatting;
using SqlParser.Ast;
using SqlParser.Dialects;

namespace SqlParser.Tests.Dialects;

public class ClickhouseDialectTests : ParserTestBase
{
    public ClickhouseDialectTests()
    {
        DefaultDialects = new[] { new ClickHouseDialect() };
    }

    [Fact]
    public void Parse_Map_Access_Expr()
    {
        var select = VerifiedOnlySelect("SELECT string_values[indexOf(string_names, 'endpoint')] FROM foos WHERE id = 'test' AND string_value[indexOf(string_name, 'app')] <> 'foo'");

        var args = new Sequence<FunctionArg>
        {
            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier("string_names"))),
            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.LiteralValue(new Value.SingleQuotedString("endpoint"))))
        };

        var selectionArgs = new Sequence<FunctionArg>
        {
            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier("string_name"))),
            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.LiteralValue(new Value.SingleQuotedString("app"))))
        };

        var expected = new Select([
            new SelectItem.UnnamedExpression(new Expression.MapAccess(new Expression.Identifier("string_values"),
            [
                new(new Expression.Function("indexOf")
                {
                    Args = new FunctionArguments.List(new FunctionArgumentList(null, args, null))
                },
                MapAccessSyntax.Bracket)
            ]))
        ])
        {
            From =
            [
                new(new TableFactor.Table("foos"))
            ],
            Selection = new Expression.BinaryOp(
                // Left
                new Expression.BinaryOp(
                    new Expression.Identifier("id"),
                    BinaryOperator.Eq,
                    new Expression.LiteralValue(new Value.SingleQuotedString("test"))),
                // Op
                BinaryOperator.And,
                // Right
                new Expression.BinaryOp(
                    new Expression.MapAccess(new Expression.Identifier("string_value"),
                    [
                        new Expression.MapAccessKey(new Expression.Function("indexOf")
                        {
                            Args = new FunctionArguments.List(new FunctionArgumentList(null, selectionArgs, null))

                        }, MapAccessSyntax.Bracket)


                    ]),
                    BinaryOperator.NotEq,
                    new Expression.LiteralValue(new Value.SingleQuotedString("foo"))
                )
            )
        };

        Assert.Equal(expected, select);
    }

    [Fact]
    public void Parse_Array_Expr()
    {
        var select = VerifiedOnlySelect("SELECT ['1', '2'] FROM test");
        var expected = new Expression.Array(new ArrayExpression([
            new Expression.LiteralValue(new Value.SingleQuotedString("1")),
            new Expression.LiteralValue(new Value.SingleQuotedString("2"))
        ]));

        Assert.Equal(expected, select.Projection.First().AsExpr());
    }

    [Fact]
    public void Parse_Array_Fn()
    {
        var select = VerifiedOnlySelect("SELECT array(x1, x2) FROM foo");

        var args = new Sequence<FunctionArg>
        {
             new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier("x1"))),
             new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier("x2")))
        };

        var expected = new Expression.Function("array")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList(null, args, null))
        };

        Assert.Equal(expected, select.Projection.First().AsExpr());

    }

    [Fact]
    public void Parse_Kill()
    {
        var statement = VerifiedStatement("KILL MUTATION 5");

        var expected = new Statement.Kill(KillType.Mutation, 5);
        Assert.Equal(expected, statement);
    }

    [Fact]
    public void Parse_Delimited_Identifiers()
    {
        var select = VerifiedOnlySelect("""
                        SELECT "alias"."bar baz", "myfun"(), "simple id" AS "column alias" FROM "a table" AS "alias"
                        """);

        var relation = (TableFactor.Table)select.From!.First().Relation!;

        Assert.Equal(new Ident("a table", Symbols.DoubleQuote), relation.Name);
        Assert.Equal(new Ident("alias", Symbols.DoubleQuote), relation.Alias!.Name);

        Assert.Equal(3, select.Projection.Count);
        var compound = new Expression.CompoundIdentifier([
            new Ident("alias", Symbols.DoubleQuote),
            new Ident("bar baz", Symbols.DoubleQuote)
        ]);
        Assert.Equal(compound, select.Projection[0].AsExpr());

        var fn = new Expression.Function(new ObjectName(new Ident("myfun", Symbols.DoubleQuote)))
        {
            Args = new FunctionArguments.List(FunctionArgumentList.Empty())
        };
        Assert.Equal(fn, select.Projection[1].AsExpr());

        var exp = (SelectItem.ExpressionWithAlias)select.Projection[2];
        var simpleId = new Expression.Identifier(new Ident("simple id", Symbols.DoubleQuote));
        var quote = new Ident("column alias", Symbols.DoubleQuote);
        Assert.Equal(simpleId, exp.Expression);
        Assert.Equal(quote, exp.Alias);

        VerifiedStatement("""
                        CREATE TABLE "foo" ("bar" "int")
                        """);

        VerifiedStatement("""
                      ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)
                      """);
    }

    [Fact]
    public void Parse_Create_Table()
    {
        VerifiedStatement("""
                     CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x")
                     """);

        VerifiedStatement("""
                          CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY "x"
                          """);

        VerifiedStatement("""
                          CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY "x" AS SELECT * FROM "t" WHERE true
                          """);
    }

    [Fact]
    public void Parse_Double_Equal()
    {
        OneStatementParsesTo("SELECT foo FROM bar WHERE buz == 'buz'", "SELECT foo FROM bar WHERE buz = 'buz'");
    }

    [Fact]
    public void Parse_Select_Star_Except()
    {
        VerifiedStatement("SELECT * EXCEPT (prev_status) FROM anomalies");
    }

    [Fact]
    public void Parse_Select_Star_Except_No_Parens()
    {
        OneStatementParsesTo("SELECT * EXCEPT prev_status FROM anomalies", "SELECT * EXCEPT (prev_status) FROM anomalies");
    }

    [Fact]
    public void Parse_Select_Replace()
    {
        VerifiedStatement("SELECT * REPLACE (i + 1 AS i) FROM columns_transformers");
    }

    [Fact]
    public void Parse_Create_View_With_Fields_Data_Types()
    {
        var view = VerifiedStatement<Statement.CreateView>("CREATE VIEW v (i \"int\", f \"String\") AS SELECT * FROM t");

        var columns = new Sequence<ViewColumnDef>
        {
            new ("i", new DataType.Custom(new ObjectName(new Ident("int", Symbols.DoubleQuote)))),
            new ("f", new DataType.Custom(new ObjectName(new Ident("String", Symbols.DoubleQuote)))),
        };

        Assert.Equal("v", view.Name);
        Assert.Equal(columns, view.Columns);

        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE VIEW v (i, f) AS SELECT * FROM t"));
    }

    [Fact]
    public void Parse_Clickhouse_Data_Types()
    {
        const string sql = """
                           CREATE TABLE table (
                           a1 UInt8, a2 UInt16, a3 UInt32, a4 UInt64, a5 UInt128, a6 UInt256, 
                           b1 Int8, b2 Int16, b3 Int32, b4 Int64, b5 Int128, b6 Int256, 
                           c1 Float32, c2 Float64, 
                           d1 Date32, d2 DateTime64(3), d3 DateTime64(3, 'UTC'), 
                           e1 FixedString(255), 
                           f1 LowCardinality(Int32)
                           ) ORDER BY (a1)
                           """;

        var canonical = sql.Replace("Int8", "INT8")
            .Replace("Int64", "INT64")
            .Replace("Float64", "FLOAT64");

        var create = OneStatementParsesTo<Statement.CreateTable>(sql, canonical).Element;
        var columns = new Sequence<ColumnDef>
        {
            new("a1", new DataType.UInt8()),
            new("a2", new DataType.UInt16()),
            new("a3", new DataType.UInt32()),
            new("a4", new DataType.UInt64()),
            new("a5", new DataType.UInt128()),
            new("a6", new DataType.UInt256()),
            new("b1", new DataType.Int8()),
            new("b2", new DataType.Int16()),
            new("b3", new DataType.Int32()),
            new("b4", new DataType.Int64()),
            new("b5", new DataType.Int128()),
            new("b6", new DataType.Int256()),
            new("c1", new DataType.Float32()),
            new("c2", new DataType.Float64()),
            new("d1", new DataType.Date32()),
            new("d2", new DataType.Datetime64(3)),
            new("d3", new DataType.Datetime64(3, "UTC")),
            new("e1", new DataType.FixedString(255)),
            new("f1", new DataType.LowCardinality(new DataType.Int32()))
        };

        Assert.Equal("table", create.Name);
        Assert.Equal(columns, create.Columns);
    }

    [Fact]
    public void Parse_Create_Table_With_Nullable()
    {
        const string sql = "CREATE TABLE table (k UInt8, `a` Nullable(String), `b` Nullable(DateTime64(9, 'UTC')), c Nullable(DateTime64(9)), d Date32 NULL) ENGINE=MergeTree ORDER BY (`k`)";

        var canonical = sql.Replace("String", "STRING");

        var create = OneStatementParsesTo<Statement.CreateTable>(sql, canonical).Element;

        var columns = new Sequence<ColumnDef>
        {
            new ("k", new DataType.UInt8()),
            new (new Ident("a", Symbols.Backtick), new DataType.Nullable(new DataType.StringType())),
            new (new Ident("b", Symbols.Backtick), new DataType.Nullable(new DataType.Datetime64(9, "UTC"))),
            new ("c", new DataType.Nullable(new DataType.Datetime64(9))),
            new ("d", new DataType.Date32(), Options:[new ColumnOptionDef(new ColumnOption.Null())]),
        };

        Assert.Equal("table", create.Name);
        Assert.Equal(columns, create.Columns);
    }

    [Fact]
    public void Parse_Create_Table_With_Nested_Data_Types()
    {
        const string sql = """
                           CREATE TABLE table ( 
                           i Nested(a Array(Int16), b LowCardinality(String)), 
                           k Array(Tuple(FixedString(128), Int128)), 
                           l Tuple(a DateTime64(9), b Array(UUID)), 
                           m Map(String, UInt16) 
                           ) ENGINE=MergeTree ORDER BY (k0)
                           """;

        var create = OneStatementParsesTo<Statement.CreateTable>(sql, "").Element;

        var columns = new Sequence<ColumnDef>
        {
            new ("i", new DataType.Nested([
                new ColumnDef("a", new DataType.Array(new ArrayElementTypeDef.Parenthesis(new DataType.Int16()))),
                new ColumnDef("b", new DataType.LowCardinality(new DataType.StringType()))
            ])),

            new ("k", new DataType.Array(new ArrayElementTypeDef.Parenthesis(new DataType.Tuple([
                new StructField(new DataType.FixedString(128)),
                new StructField(new DataType.Int128())
            ])))),

            new ("l", new DataType.Tuple([
                new StructField(new DataType.Datetime64(9), "a"),
                new StructField(new DataType.Array(new ArrayElementTypeDef.Parenthesis(new DataType.Uuid())), "b"),
            ])),

            new ("m", new DataType.Map(new DataType.StringType(), new DataType.UInt16()))
        };

        Assert.Equal("table", create.Name);
        Assert.Equal(columns, create.Columns);
    }

    [Fact]
    public void Parse_Create_Table_With_Primary_Key()
    {
        const string sql = """
                CREATE TABLE db.table (`i` INT, `k` INT) 
                ENGINE=SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') 
                PRIMARY KEY tuple(i) 
                ORDER BY tuple(i)
                """;
        DefaultDialects = [new ClickHouseDialect()];
        var statement = VerifiedStatement<Statement.CreateTable>(sql).Element;

        Assert.Equal("db.table", statement.Name);
        Assert.Equal([
            new ColumnDef(new Ident("i", Symbols.Backtick), new DataType.Int()),
            new ColumnDef(new Ident("k", Symbols.Backtick), new DataType.Int()),
        ], statement.Columns);

        Assert.Equal(new TableEngine("SharedMergeTree",
            [
                new Ident("/clickhouse/tables/{uuid}/{shard}", Symbols.SingleQuote),
                new Ident("{replica}", Symbols.SingleQuote)
            ]),
            statement.Engine);

        var orderByFn = (Expression.Function)((OneOrManyWithParens<Expression>.One)statement.OrderBy!).Value;
        AssertFunction((Expression.Function)statement.PrimaryKey!, "tuple", "i");
        AssertFunction(orderByFn, "tuple", "i");

        Assert.Throws<ParserException>(() => ParseSqlStatements("""
                                               CREATE TABLE db.table (`i` Int, `k` Int) 
                                               ORDER BY tuple(i), tuple(k)
                                               """));
        return;

        static void AssertFunction(Expression.Function actual, string name, string arg)
        {
            Assert.Equal(new ObjectName(new Ident(name)), actual.Name);
            Assert.Equal(
                new FunctionArguments.List(new FunctionArgumentList(null, [
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier(new Ident(arg))))
                ], null)),
                actual.Args);
        }
    }

    [Fact]
    public void Parse_Create_Materialize_View()
    {
        const string sql = """
                           CREATE MATERIALIZED VIEW analytics.monthly_aggregated_data_mv 
                           TO analytics.monthly_aggregated_data 
                           AS SELECT toDate(toStartOfMonth(event_time)) 
                           AS month, domain_name, sumState(count_views) 
                           AS sumCountViews FROM analytics.hourly_data 
                           GROUP BY domain_name, month
                           """;

        VerifiedStatement(sql);
    }

    [Fact]
    public void Parse_Select_Parametric_Function()
    {
        var projection = VerifiedStatement("SELECT HISTOGRAM(0.5, 0.6)(x, y) FROM t").AsQuery()!.Body.AsSelect().Projection;

        var expected = new SelectItem.UnnamedExpression(new Expression.Function("HISTOGRAM")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList(null, [
                new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier("x"))),
                new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier("y"))),
            ], null)),
            Parameters = new FunctionArguments.List(new FunctionArgumentList(null, [
                new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.LiteralValue(new Value.Number("0.5")))),
                new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.LiteralValue(new Value.Number("0.6"))))
            ], null))
        });
        Assert.Equal(expected, projection[0]);
    }

    [Fact]
    public void Parse_Group_By_With_Modifier()
    {
        var clauses = new[] { "x", "a, b", "ALL" };
        var modifiers = new[]{
            "WITH ROLLUP",
            "WITH CUBE",
            "WITH TOTALS",
            "WITH ROLLUP WITH CUBE",
        };

        var expectedModifiers = new Sequence<GroupByWithModifier>[]{
            [GroupByWithModifier.Rollup],
            [GroupByWithModifier.Cube],
            [GroupByWithModifier.Totals],
            [GroupByWithModifier.Rollup, GroupByWithModifier.Cube],
        };

        foreach (var clause in clauses)
        {
            foreach (var (modifier, expectedModifier) in modifiers.Zip(expectedModifiers))
            {
                var sql = $"SELECT * FROM T GROUP BY {clause} {modifier}";

                var statement = VerifiedStatement(sql);
                var groupBy = statement.AsQuery()!.Body.AsSelect().GroupBy;

                if (clause == "ALL")
                {
                    Assert.Equal(new GroupByExpression.All(expectedModifier), groupBy);
                }
                else
                {
                    var columnNames = new Sequence<Expression>(clause.Split(", ").Select(c => new Expression.Identifier(c)));
                    var expected = new GroupByExpression.Expressions(columnNames, expectedModifier);
                    Assert.Equal(expected, groupBy);
                }
            }
        }

        var invalidClauses = new[]
        {
            "SELECT * FROM t GROUP BY x WITH",
            "SELECT * FROM t GROUP BY x WITH ROLLUP CUBE",
            "SELECT * FROM t GROUP BY x WITH WITH ROLLUP",
            "SELECT * FROM t GROUP BY WITH ROLLUP"
        };

        foreach (var invalid in invalidClauses)
        {
            Assert.Throws<ParserException>(() => ParseSqlStatements(invalid));
        }
    }

    [Fact]
    public void Parse_Settings_In_Query()
    {
        var query = VerifiedStatement("SELECT * FROM t SETTINGS max_threads = 1, max_block_size = 10000").AsQuery()!;

        Assert.Equal(
            [
                new ("max_threads", new Value.Number("1")),
                new ("max_block_size", new Value.Number("10000"))
            ],
            query.Settings);

        foreach (var sql in new[]
                 {
                     "SELECT * FROM t SETTINGS a",
                     "SELECT * FROM t SETTINGS a=",
                     "SELECT * FROM t SETTINGS a=1, b",
                     "SELECT * FROM t SETTINGS a=1, b=",
                     "SELECT * FROM t SETTINGS a=1, b=c",
                 })
        {
            Assert.Throws<ParserException>(() => ParseSqlStatements(sql));
        }
    }

    [Fact]
    public void Test_Prewhere()
    {
        var select = VerifiedStatement("SELECT * FROM t PREWHERE x = 1 WHERE y = 2").AsQuery()!.Body.AsSelect();

        var expected = new Expression.BinaryOp(
            new Expression.Identifier("x"),
            BinaryOperator.Eq,
            new Expression.LiteralValue(new Value.Number("1"))
        );

        Assert.Equal(expected, select.PreWhere);

        expected = new Expression.BinaryOp(
            new Expression.Identifier("y"),
            BinaryOperator.Eq,
            new Expression.LiteralValue(new Value.Number("2"))
        );

        Assert.Equal(expected, select.Selection);
    }

    [Fact]
    public void Test_Query_With_Format_Clause()
    {
        var formatOptions = new Sequence<string> { "TabSeparated", "JSONCompact", "NULL" };
        foreach (var format in formatOptions)
        {
            var sql = $"SELECT * FROM t FORMAT {format}";
            var query = VerifiedStatement(sql).AsQuery()!;

            if (format == "NULL")
            {
                Assert.Equal(new FormatClause.Null(), query.FormatClause);
            }
            else
            {
                Assert.Equal(new FormatClause.Identifier(format), query.FormatClause);
            }
        }

        var invalidCases =new []{
            "SELECT * FROM t FORMAT",
            "SELECT * FROM t FORMAT TabSeparated JSONCompact",
            "SELECT * FROM t FORMAT TabSeparated TabSeparated",
        };

        foreach(var sql in invalidCases)
        {
            Assert.Throws<ParserException>(() =>  ParseSqlStatements(sql));
        }
    }
}