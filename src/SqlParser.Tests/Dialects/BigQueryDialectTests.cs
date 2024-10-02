using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.Expression;
// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests.Dialects;

public class BigQueryDialectTests : ParserTestBase
{
    public BigQueryDialectTests()
    {
        DefaultDialects = new[] { new BigQueryDialect() };
    }

    [Fact]
    public void Parse_Literal_String()
    {
        const string sql = """"
                           SELECT
                            'single',
                            "double",
                            '''triple-single''',
                            """triple-double""",
                            'single\'escaped',
                            '''triple-single\'escaped''',
                            '''triple-single'unescaped''',
                            "double\"escaped",
                            """triple-double\"escaped""",
                            """triple-double"unescaped"""
                           """";

        var select = VerifiedOnlySelect(sql);

        Assert.Equal(10, select.Projection.Count);
        Assert.Equal(new LiteralValue(new Value.SingleQuotedString("single")), select.Projection[0].AsExpr());
        Assert.Equal(new LiteralValue(new Value.DoubleQuotedString("double")), select.Projection[1].AsExpr());
        Assert.Equal(new LiteralValue(new Value.TripleSingleQuotedString("triple-single")), select.Projection[2].AsExpr());
        Assert.Equal(new LiteralValue(new Value.TripleDoubleQuotedString("triple-double")), select.Projection[3].AsExpr());
        Assert.Equal(new LiteralValue(new Value.SingleQuotedString("single\\'escaped")), select.Projection[4].AsExpr());
        Assert.Equal(new LiteralValue(new Value.TripleSingleQuotedString("triple-single\\'escaped")), select.Projection[5].AsExpr());
        Assert.Equal(new LiteralValue(new Value.TripleSingleQuotedString("triple-single'unescaped")), select.Projection[6].AsExpr());
        Assert.Equal(new LiteralValue(new Value.DoubleQuotedString("double\\\"escaped")), select.Projection[7].AsExpr());
        Assert.Equal(new LiteralValue(new Value.TripleDoubleQuotedString("triple-double\\\"escaped")), select.Projection[8].AsExpr());
        Assert.Equal(new LiteralValue(new Value.TripleDoubleQuotedString("triple-double\"unescaped")), select.Projection[9].AsExpr());
    }

    [Fact]
    public void Parse_Byte_Literal()
    {
        var sql = """"
                  SELECT
                   B'abc',
                   B"abc",
                   B'f\(abc,(.*),def\)',
                   B"f\(abc,(.*),def\)",
                   B'''abc''',
                   B"""abc"""
                  """";

        var statement = VerifiedStatement(sql);
        var projection = statement.AsQuery()!.Body.AsSelect().Projection;

        Assert.Equal(6, projection.Count);
        Assert.Equal(new LiteralValue(new Value.SingleQuotedByteStringLiteral("abc")), projection[0].AsExpr());
        Assert.Equal(new LiteralValue(new Value.DoubleQuotedByteStringLiteral("abc")), projection[1].AsExpr());
        Assert.Equal(new LiteralValue(new Value.SingleQuotedByteStringLiteral("f\\(abc,(.*),def\\)")), projection[2].AsExpr());
        Assert.Equal(new LiteralValue(new Value.DoubleQuotedByteStringLiteral("f\\(abc,(.*),def\\)")), projection[3].AsExpr());
        Assert.Equal(new LiteralValue(new Value.TripleSingleQuotedByteStringLiteral("abc")), projection[4].AsExpr());
        Assert.Equal(new LiteralValue(new Value.TripleDoubleQuotedByteStringLiteral("abc")), projection[5].AsExpr());

        OneStatementParsesTo(
            """"
            SELECT b'123', b"123", b'''123''', b"""123"""
            """",
            """"
            SELECT B'123', B"123", B'''123''', B"""123"""
            """");
    }

    [Fact]
    public void Parse_Raw_Literal()
    {
        const string sql = """"
                           SELECT
                            R'abc',
                            R"abc",
                            R'f\(abc,(.*),def\)',
                            R"f\(abc,(.*),def\)",
                            R'''abc''',
                            R"""abc"""
                           """";

        var statement = VerifiedStatement(sql);
        var projection = statement.AsQuery()!.Body.AsSelect().Projection;

        Assert.Equal(6, projection.Count);
        Assert.Equal(new LiteralValue(new Value.SingleQuotedRawStringLiteral("abc")), projection[0].AsExpr());
        Assert.Equal(new LiteralValue(new Value.DoubleQuotedRawStringLiteral("abc")), projection[1].AsExpr());
        Assert.Equal(new LiteralValue(new Value.SingleQuotedRawStringLiteral("f\\(abc,(.*),def\\)")), projection[2].AsExpr());
        Assert.Equal(new LiteralValue(new Value.DoubleQuotedRawStringLiteral("f\\(abc,(.*),def\\)")), projection[3].AsExpr());
        Assert.Equal(new LiteralValue(new Value.TripleSingleQuotedRawStringLiteral("abc")), projection[4].AsExpr());
        Assert.Equal(new LiteralValue(new Value.TripleDoubleQuotedRawStringLiteral("abc")), projection[5].AsExpr());
    }

    [Fact]
    public void Parse_Table_Identifiers()
    {
        Test("`spa ce`", new Ident[] { new("spa ce", '`') });
        Test("`!@#$%^&*()-=_+`", new Ident[] { new("!@#$%^&*()-=_+", '`') });
        Test("_5abc.dataField", new Ident[] { "_5abc", "dataField" });
        Test("`5abc`.dataField", new Ident[] { new("5abc", '`'), "dataField" });
        Assert.Throws<ParserException>(() => VerifiedStatement("SELECT 1 FROM 5abc.dataField"));
        Test("abc5.dataField", new Ident[] { "abc5", "dataField" });
        Assert.Throws<ParserException>(() => VerifiedStatement("SELECT 1 FROM abc5!.dataField"));
        Test("`GROUP`.dataField", new Ident[] { new("GROUP", '`'), "dataField" });
        Test("abc5.GROUP", new Ident[] { "abc5", "GROUP" });
        Test("`foo.bar.baz`", new[]
        {
            new Ident("foo", Symbols.Backtick),
            new Ident("bar", Symbols.Backtick),
            new Ident("baz", Symbols.Backtick),
        }, "`foo`.`bar`.`baz`");

        Test("`foo.bar`.`baz`", new[]
        {
            new Ident("foo", Symbols.Backtick),
            new Ident("bar", Symbols.Backtick),
            new Ident("baz", Symbols.Backtick),
        }, "`foo`.`bar`.`baz`");

        Test("`foo`.`bar.baz`", new[]
        {
            new Ident("foo", Symbols.Backtick),
            new Ident("bar", Symbols.Backtick),
            new Ident("baz", Symbols.Backtick),
        }, "`foo`.`bar`.`baz`");

        Test("`foo`.`bar`.`baz`", new[]
        {
            new Ident("foo", Symbols.Backtick),
            new Ident("bar", Symbols.Backtick),
            new Ident("baz", Symbols.Backtick),
        }, "`foo`.`bar`.`baz`");

        Test("`5abc.dataField`", new[]
        {
            new Ident("5abc", Symbols.Backtick),
            new Ident("dataField", Symbols.Backtick),
        }, "`5abc`.`dataField`");

        Test("`_5abc.da-sh-es`", new[]
        {
            new Ident("_5abc", Symbols.Backtick),
            new Ident("da-sh-es", Symbols.Backtick),
        }, "`_5abc`.`da-sh-es`");

        Test("foo-bar.baz-123", new Ident[] { "foo-bar", "baz-123" }, "foo-bar.baz-123");

        Assert.Throws<ParserException>(() => VerifiedStatement("foo-`bar`"));
        Assert.Throws<ParserException>(() => VerifiedStatement("`foo`-bar"));
        Assert.Throws<ParserException>(() => VerifiedStatement("foo-123a"));
        Assert.Throws<ParserException>(() => VerifiedStatement("foo - bar"));
        Assert.Throws<ParserException>(() => VerifiedStatement("123-bar"));
        Assert.Throws<ParserException>(() => VerifiedStatement("bar-"));

        // Parses a table identifier ident and verifies that re-serializing the
        // parsed identifier produces the original ident string.
        //
        // In some cases, re-serializing the result of the parsed ident is not
        // expected to produce the original ident string. canonical is provided
        // instead as the canonical representation of the identifier for comparison.
        // For example, re-serializing the result of ident `foo.bar` produces
        // the equivalent canonical representation `foo`.`bar`
        void Test(string ident, Sequence<Ident> names, string? canonical = null)
        {
            var select = canonical != null
                ? VerifiedOnlySelectWithCanonical($"SELECT 1 FROM {ident}", $"SELECT 1 FROM {canonical}")
                : VerifiedOnlySelect($"SELECT 1 FROM {ident}");

            var expected = new TableWithJoins[]
            {
                new (new TableFactor.Table(new ObjectName(names)))
            };
            Assert.Equal(expected, select.From!);
        }
    }

    [Fact]
    public void Parse_Join_Constraint_Unnest_Alias()
    {
        var select = VerifiedOnlySelect("SELECT * FROM t1 JOIN UNNEST(t1.a) AS f ON c1 = c2");
        var joins = select.From!.Single().Joins;

        var expected = new Join[]
        {
            new (new TableFactor.UnNest([new CompoundIdentifier(new Ident[] { "t1", "a" })])
                {
                    Alias = new TableAlias("f"),
                },
                new JoinOperator.Inner(new JoinConstraint.On(
                    new BinaryOp(
                        new Identifier("c1"),
                        BinaryOperator.Eq,
                        new Identifier("c2")
                    )))
            )
        };

        Assert.Equal(expected, joins!);
    }

    [Fact]
    public void Parse_Trailing_Comma()
    {
        foreach (var (sql, canonical) in new[]{
                     ("SELECT a,", "SELECT a"),
                     ("SELECT 1,", "SELECT 1"),
                     ("SELECT 1,2,", "SELECT 1, 2"),
                     ("SELECT a, b,", "SELECT a, b"),
                     ("SELECT a, b AS c,", "SELECT a, b AS c"),
                     ("SELECT a, b AS c, FROM t", "SELECT a, b AS c FROM t"),
                     ("SELECT a, b, FROM t", "SELECT a, b FROM t"),
                     ("SELECT a, b, LIMIT 1", "SELECT a, b LIMIT 1"),
                     ("SELECT a, (SELECT 1, )", "SELECT a, (SELECT 1)"),
                 })
        {
            OneStatementParsesTo(sql, canonical);
        }
    }

    [Fact]
    public void Parse_Cast_Type()
    {
        VerifiedOnlySelect("SELECT SAFE_CAST(1 AS INT64)");
    }

    [Fact]
    public void Parse_Cast_Date_Format()
    {
        VerifiedOnlySelect("SELECT CAST(date_valid_from AS DATE FORMAT 'YYYY-MM-DD') AS date_valid_from FROM foo");
    }


    [Fact]
    public void Parse_Cast_Time_Format()
    {
        VerifiedOnlySelect("SELECT CAST(TIME '21:30:00' AS STRING FORMAT 'PM') AS date_time_to_string");
    }

    [Fact]
    public void Parse_Cast_Timestamp_Format_Tz()
    {
        VerifiedOnlySelect("SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'TZH' AT TIME ZONE 'Asia/Kolkata') AS date_time_to_string");
    }

    [Fact]
    public void Parse_Cast_String_To_Bytes_Format()
    {
        VerifiedOnlySelect("SELECT CAST('Hello' AS BYTES FORMAT 'ASCII') AS string_to_bytes");
    }

    [Fact]
    public void Parse_Cast_Bytes_To_String_Format()
    {
        VerifiedOnlySelect("SELECT CAST(B'\\x48\\x65\\x6c\\x6c\\x6f' AS STRING FORMAT 'ASCII') AS bytes_to_string");
    }

    [Fact]
    public void Parse_Array_Agg_Func()
    {
        var supportedDialects = new List<Dialect>
        {
            new GenericDialect(),
            new PostgreSqlDialect(),
            new MsSqlDialect(),
            new AnsiDialect(),
            new HiveDialect()
        };

        foreach (var sql in new[]{
                     "SELECT ARRAY_AGG(x ORDER BY x) AS a FROM T",
                     "SELECT ARRAY_AGG(x ORDER BY x LIMIT 2) FROM tbl",
                     "SELECT ARRAY_AGG(DISTINCT x ORDER BY x LIMIT 2) FROM tbl",
                 })
        {
            VerifiedStatement(sql, supportedDialects);
        }
    }

    [Fact]
    public void Parse_Map_Access_Expr()
    {
        var expression = VerifiedExpr("users[-1][safe_offset(2)].a.b");

        var args = new Sequence<FunctionArg>
        {
            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.Number("2"))))
        };

        var expected = new MapAccess(new Identifier("users"), [
            new(new UnaryOp(new LiteralValue(new Value.Number("1")), UnaryOperator.Minus), MapAccessSyntax.Bracket),
            new(new Function("safe_offset")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList(Args: args))
            }, MapAccessSyntax.Bracket),

            new(new CompoundIdentifier(["a", "b"]), MapAccessSyntax.Period)
        ]);

        Assert.Equal(expected, expression);

        VerifiedOnlySelect("SELECT myfunc()[-1].a[SAFE_OFFSET(2)].b");
    }

    [Fact]
    public void Parse_Table_Time_Travel()
    {
        var dialect = new[] { new BigQueryDialect() };
        const string version = "2023-08-18 23:08:18";
        var sql = $"SELECT 1 FROM t1 FOR SYSTEM_TIME AS OF '{version}'";
        var select = VerifiedOnlySelect(sql, dialect);

        var from = select.From;

        var expected = new Sequence<TableWithJoins>
        {
            new (new TableFactor.Table("t1")
            {
                Version = new TableVersion.ForSystemTimeAsOf(new LiteralValue(new Value.SingleQuotedString(version)))
            })
        };

        Assert.Equal(expected, from);

        sql = "SELECT 1 FROM t1 FOR SYSTEM TIME AS OF 'some_timestamp'";
        Assert.Throws<ParserException>(() => ParseSqlStatements(sql, dialect));
    }

    [Fact]
    public void Test_BigQuery_Trim()
    {
        var sql = """
                  SELECT customer_id, TRIM(item_price_id, '"', "a") AS item_price_id FROM models_staging.subscriptions
                  """;

        VerifiedStatement(sql);

        sql = "SELECT TRIM('xyz', 'a')";

        var select = VerifiedOnlySelect(sql, new[] { new BigQueryDialect() });
        var expected = new Trim(new LiteralValue(new Value.SingleQuotedString("xyz")),
            TrimWhereField.None,
            TrimCharacters:
            [
                new LiteralValue(new Value.SingleQuotedString("a"))
            ]);

        Assert.Equal(expected, select.Projection.First().AsExpr());

        Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT TRIM('xyz' 'a')", new[] { new BigQueryDialect() }));
    }

    [Fact]
    public void Parse_Nested_Data_Types()
    {
        const string sql = "CREATE TABLE table (x STRUCT<a ARRAY<INT64>, b BYTES(42)>, y ARRAY<STRUCT<INT64>>)";

        var create = (Statement.CreateTable)OneStatementParsesTo(sql, sql, new[] { new BigQueryDialect() });

        var columns = new Sequence<ColumnDef>
        {
            new ("x", new DataType.Struct([
                new(new DataType.Array(new ArrayElementTypeDef.AngleBracket(new DataType.Int64())), "a"),
                new(new DataType.Bytes(42), "b")
            ], StructBracketKind.AngleBrackets)),
            new("y", new DataType.Array(new ArrayElementTypeDef.AngleBracket(new DataType.Struct([
               new(new DataType.Int64())
            ], StructBracketKind.AngleBrackets))))
        };
        var expected = new Statement.CreateTable(new CreateTable("table", columns));
        Assert.Equal(expected, create);
    }

    [Fact]
    public void Parse_Invalid_Brackets()
    {
        var sql = "SELECT STRUCT<INT64>>(NULL)";
        Assert.Throws<ParserException>(() => ParseSqlStatements(sql));

        sql = "SELECT STRUCT<STRUCT<INT64>>>(NULL)";
        Assert.Throws<ParserException>(() => ParseSqlStatements(sql));

        sql = "CREATE TABLE table (x STRUCT<STRUCT<INT64>>>)";
        Assert.Throws<ParserException>(() => ParseSqlStatements(sql));
    }

    [Fact]
    public void Parse_Tuple_Struct_Literal()
    {
        const string sql = "SELECT (1, 2, 3), (1, 1.0, '123', true)";
        var select = VerifiedOnlySelect(sql);

        var expected = new Expression.Tuple([
            new LiteralValue(new Value.Number("1")),
            new LiteralValue(new Value.Number("2")),
            new LiteralValue(new Value.Number("3"))
        ]);
        Assert.Equal(expected, select.Projection.First().AsExpr());

        expected = new Expression.Tuple([
            new LiteralValue(new Value.Number("1")),
            new LiteralValue(new Value.Number("1.0")),
            new LiteralValue(new Value.SingleQuotedString("123")),
            new LiteralValue(new Value.Boolean(true))
        ]);

        Assert.Equal(expected, select.Projection.Skip(1).First().AsExpr());
    }

    [Fact]
    public void Parse_Typeless_Struct_Syntax()
    {
        var sql = "SELECT STRUCT(1, 2, 3), STRUCT('abc'), STRUCT(1, t.str_col), STRUCT(1 AS a, 'abc' AS b), STRUCT(str_col AS abc)";
        var select = VerifiedOnlySelect(sql);

        Expression expected = new Struct([
            new LiteralValue(new Value.Number("1")),
            new LiteralValue(new Value.Number("2")),
            new LiteralValue(new Value.Number("3"))
        ], new Sequence<StructField>());
        Assert.Equal(expected, select.Projection.First().AsExpr());

        expected = new Struct([
            new LiteralValue(new Value.SingleQuotedString("abc"))
        ], []);
        Assert.Equal(expected, select.Projection.Skip(1).First().AsExpr());

        expected = new Struct([
            new LiteralValue(new Value.Number("1")),
            new CompoundIdentifier(["t", "str_col"])
        ], []);
        Assert.Equal(expected, select.Projection.Skip(2).First().AsExpr());

        expected = new Struct([
            new Named(new LiteralValue(new Value.Number("1")), "a"),
            new Named(new LiteralValue(new Value.SingleQuotedString("abc")), "b")
        ], []);
        Assert.Equal(expected, select.Projection.Skip(3).First().AsExpr());

        expected = new Struct([new Named(new Identifier("str_col"), "abc")], new Sequence<StructField>());
        Assert.Equal(expected, select.Projection.Skip(4).First().AsExpr());
    }

    [Fact]
    public void Parse_Delete_Statement()
    {
        var delete = (Statement.Delete)VerifiedStatement("DELETE \"table\" WHERE 1");
        var relation = (delete.DeleteOperation.From as FromTable.WithoutKeyword)?.From.First().Relation;

        var expected = new TableFactor.Table(new ObjectName(new Ident("table", Symbols.DoubleQuote)));

        Assert.Equal(expected, relation);
    }

    [Fact]
    public void Parse_Create_View_If_Not_Exists()
    {
        const string sql = "CREATE VIEW IF NOT EXISTS mydataset.newview AS SELECT foo FROM bar";
        var create = VerifiedStatement<Statement.CreateView>(sql);

        Assert.Equal("mydataset.newview", create.Name);
        Assert.Equal("SELECT foo FROM bar", create.Query.ToSql());
        Assert.False(create.Materialized);
        Assert.False(create.OrReplace);
        Assert.Equal(new CreateTableOptions.None(), create.Options);
        Assert.Null(create.Comment);
        Assert.False(create.WithNoSchemaBinding);
        Assert.True(create.IfNotExists);
        Assert.False(create.Temporary);
    }
    [Fact]
    public void Test_Select_As_Struct()
    {
        VerifiedOnlySelect("SELECT * FROM (SELECT AS VALUE STRUCT(123 AS a, false AS b))");
        var select = VerifiedOnlySelect("SELECT AS STRUCT 1 AS a, 2 AS b");
        Assert.IsType<ValueTableMode.AsStruct>(select.ValueTableMode);
    }
    [Fact]
    public void Test_Select_As_Value()
    {
        VerifiedOnlySelect("SELECT * FROM (SELECT AS VALUE STRUCT(5 AS star_rating, false AS up_down_rating))");
        var select = VerifiedOnlySelect("SELECT AS VALUE STRUCT(1 AS a, 2 AS b) AS xyz");
        Assert.IsType<ValueTableMode.AsValue>(select.ValueTableMode);
    }

    [Fact]
    public void Parse_Big_Query_Declare()
    {
        List<(string Sql, Sequence<Ident> Idents, DataType? DataType, DeclareAssignment? Assignment)> queries =
        [
            ("DECLARE x INT64", ["x"], new DataType.Int64(), null),
            ("DECLARE x INT64 DEFAULT 42", ["x"], new DataType.Int64(), new DeclareAssignment.Default(new LiteralValue(new Value.Number("42")))),
            ("DECLARE x, y, z INT64 DEFAULT 42", ["x", "y", "z"], new DataType.Int64(), new DeclareAssignment.Default(new LiteralValue(new Value.Number("42")))),
            ("DECLARE x DEFAULT 42", ["x"], null, new DeclareAssignment.Default(new LiteralValue(new Value.Number("42"))))
        ];

        foreach (var query in queries)
        {
            var declare = (Statement.Declare)VerifiedStatement(query.Sql);
            Assert.Single(declare.Statements);
            var statement = declare.Statements[0];
            Assert.Equal(query.Idents, statement.Names);
            Assert.Equal(query.DataType, statement.DataType);
            Assert.Equal(query.Assignment, statement.Assignment);
        }

        Assert.Throws<ParserException>(() => { ParseSqlStatements("DECLARE x"); });
        Assert.Throws<ParserException>(() => { ParseSqlStatements("DECLARE x 42"); });
    }

    [Fact]
    public void Parse_Create_View_With_Unquoted_Hyphen()
    {
        var create = VerifiedStatement<Statement.CreateView>("CREATE VIEW IF NOT EXISTS my-pro-ject.mydataset.myview AS SELECT 1");
        Assert.Equal("my-pro-ject.mydataset.myview", create.Name);
        Assert.Equal("SELECT 1", create.Query.ToSql());
    }

    [Fact]
    public void Parse_Create_Table_With_Unquoted_Hyphen()
    {
        var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE my-pro-ject.mydataset.mytable (x INT64)");

        var name = new ObjectName(["my-pro-ject", "mydataset", "mytable"]);
        var columns = new Sequence<ColumnDef>
        {
            new ("x", new DataType.Int64())
        };

        Assert.Equal(name, create.Element.Name);
        Assert.Equal(columns, create.Element.Columns);
    }

    [Fact]
    public void Parse_Extract_Weekday()
    {
        var select = VerifiedOnlySelect("SELECT EXTRACT(WEEK(MONDAY) FROM d)");

        var expected = new Extract(new Identifier("d"), new DateTimeField.Week("MONDAY"), ExtractSyntax.From);

        Assert.Equal(expected, select.Projection.First().AsExpr());
    }

    [Fact]
    public void Test_Any_Value()
    {
        VerifiedExpr("ANY_VALUE(fruit)");
        VerifiedExpr("ANY_VALUE(fruit) OVER (ORDER BY LENGTH(fruit) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)");
        VerifiedExpr("ANY_VALUE(fruit HAVING MAX sold)");
        VerifiedExpr("ANY_VALUE(fruit HAVING MIN sold)");
    }

    [Fact]
    public void Parse_Create_Table_With_Options()
    {
        var sql = """
              CREATE TABLE mydataset.newtable
               (x INT64 NOT NULL OPTIONS(description = "field x"),
               y BOOL OPTIONS(description = "field y"))
               PARTITION BY _PARTITIONDATE
               CLUSTER BY userid, age
               OPTIONS(partition_expiration_days = 1, description = "table option description")
              """;
        var create = VerifiedStatement<Statement.CreateTable>(sql).Element;

        var columns = new Sequence<ColumnDef>
        {
            new ("x", new DataType.Int64(), Options:
            [
                new (new ColumnOption.NotNull()),
                new (new ColumnOption.Options([
                    new SqlOption.KeyValue("description", new LiteralValue(new Value.DoubleQuotedString("field x")))
                ])),
            ]),
            new ("y", new DataType.Bool(), Options:
            [
                new (new ColumnOption.Options([
                    new SqlOption.KeyValue("description", new LiteralValue(new Value.DoubleQuotedString("field y")))
                ])),
            ]),
        };

        Assert.Equal(new ObjectName(["mydataset", "newtable"]), create.Name);
        Assert.Equal(columns, create.Columns);
        Assert.Equal(new Identifier("_PARTITIONDATE"), create.PartitionBy);
        Assert.Equal(new WrappedCollection<Ident>.NoWrapping(["userid", "age"]), create.ClusterBy);
        Assert.Equal(new Sequence<SqlOption>
        {
            new SqlOption.KeyValue ("partition_expiration_days", new LiteralValue(new Value.Number("1"))),
            new SqlOption.KeyValue("description", new LiteralValue(new Value.DoubleQuotedString("table option description")))
        }, create.Options);

        sql = """
              CREATE TABLE mydataset.newtable
               (x INT64 NOT NULL OPTIONS(description = "field x"),
               y BOOL OPTIONS(description = "field y"))
               CLUSTER BY userid
               OPTIONS(partition_expiration_days = 1,
               description = "table option description")
              """;

        VerifiedStatement(sql);
    }

    [Fact]
    public void Parse_Typed_Struct_Syntax_BigQuery()
    {
        var sql = "SELECT STRUCT<INT64>(5), STRUCT<x INT64, y STRING>(1, t.str_col), STRUCT<arr ARRAY<FLOAT64>, str STRUCT<BOOL>>(nested_col)";

        var select = VerifiedOnlySelect(sql);

        Assert.Equal(3, select.Projection.Count);
        Assert.Equal(new Struct([
            new LiteralValue(new Value.Number("5"))
        ], [
            new StructField(new DataType.Int64())
        ]), select.Projection[0].AsExpr());

        Assert.Equal(new Struct([
            new LiteralValue(new Value.Number("1")),
            new CompoundIdentifier(["t", "str_col"])
        ], [
            new StructField(new DataType.Int64(), "x"),
            new StructField(new DataType.StringType(), "y"),
        ]), select.Projection[1].AsExpr());

        Assert.Equal(new Struct([
          new Identifier("nested_col")
        ], [
            new StructField(new DataType.Array(new ArrayElementTypeDef.AngleBracket(new DataType.Float64())), "arr"),
            new StructField(new DataType.Struct([
                new StructField(new DataType.Bool())
            ], StructBracketKind.AngleBrackets), "str"),
        ]), select.Projection[2].AsExpr());

        sql = "SELECT STRUCT<x STRUCT, y ARRAY<STRUCT>>(nested_col)";

        select = VerifiedOnlySelect(sql);

        Assert.Single(select.Projection);
        Assert.Equal(new Struct([
            new Identifier("nested_col")
        ], [
            new StructField(new DataType.Struct([], StructBracketKind.AngleBrackets), "x"),
            new StructField(new DataType.Array(new ArrayElementTypeDef.AngleBracket(new DataType.Struct([], StructBracketKind.AngleBrackets))), "y")
        ]), select.Projection[0].AsExpr());

        sql = "SELECT STRUCT<BOOL>(true), STRUCT<BYTES(42)>(B'abc')";
        select = VerifiedOnlySelect(sql);
        Assert.Equal(2, select.Projection.Count);

        Assert.Equal(new Struct([
            new LiteralValue(new Value.Boolean(true))
        ], [
            new StructField(new DataType.Bool())
        ]), select.Projection[0].AsExpr());

        Assert.Equal(new Struct([
            new LiteralValue(new Value.SingleQuotedByteStringLiteral("abc"))
        ], [
            new StructField(new DataType.Bytes(42))
        ]), select.Projection[1].AsExpr());

        sql = "SELECT STRUCT<DATE>('2011-05-05'), STRUCT<DATETIME>(DATETIME '1999-01-01 01:23:34.45'), STRUCT<FLOAT64>(5.0), STRUCT<INT64>(1)";
        select = VerifiedOnlySelect(sql);
        Assert.Equal(4, select.Projection.Count);

        Assert.Equal(new Struct([
            new LiteralValue(new Value.SingleQuotedString("2011-05-05"))
        ], [
            new StructField(new DataType.Date())
        ]), select.Projection[0].AsExpr());

        Assert.Equal(new Struct([
            new TypedString("1999-01-01 01:23:34.45", new DataType.Datetime())
        ], [
            new StructField(new DataType.Datetime())
        ]), select.Projection[1].AsExpr());

        Assert.Equal(new Struct([
            new LiteralValue(new Value.Number("5.0"))
        ], [
            new StructField(new DataType.Float64())
        ]), select.Projection[2].AsExpr());

        Assert.Equal(new Struct([
            new LiteralValue(new Value.Number("1"))
        ], [
            new StructField(new DataType.Int64())
        ]), select.Projection[3].AsExpr());

        sql = "SELECT STRUCT<INTERVAL>(INTERVAL '1' MONTH), STRUCT<JSON>(JSON '{\"class\" : {\"students\" : [{\"name\" : \"Jane\"}]}}')";
        select = VerifiedOnlySelect(sql);
        Assert.Equal(2, select.Projection.Count);

        Assert.Equal(new Struct([
            new Interval(new LiteralValue(new Value.SingleQuotedString("1")), new DateTimeField.Month())
        ], [
            new StructField(new DataType.Interval())
        ]), select.Projection[0].AsExpr());

        Assert.Equal(new Struct([
            new TypedString("{\"class\" : {\"students\" : [{\"name\" : \"Jane\"}]}}", new DataType.Json())
        ], [
            new StructField(new DataType.Json())
        ]), select.Projection[1].AsExpr());


        sql = "SELECT STRUCT<STRING(42)>('foo'), STRUCT<TIMESTAMP>(TIMESTAMP '2008-12-25 15:30:00 America/Los_Angeles'), STRUCT<TIME>(TIME '15:30:00')";
        select = VerifiedOnlySelect(sql);
        Assert.Equal(3, select.Projection.Count);

        Assert.Equal(new Struct([
            new LiteralValue(new Value.SingleQuotedString("foo"))
        ], [
            new StructField(new DataType.StringType(42))
        ]), select.Projection[0].AsExpr());

        Assert.Equal(new Struct([
            new TypedString("2008-12-25 15:30:00 America/Los_Angeles", new DataType.Timestamp(TimezoneInfo.None))
        ], [
            new StructField(new DataType.Timestamp(TimezoneInfo.None))
        ]), select.Projection[1].AsExpr());

        Assert.Equal(new Struct([
            new TypedString("15:30:00", new DataType.Time(TimezoneInfo.None))
        ], [
            new StructField(new DataType.Time(TimezoneInfo.None))
        ]), select.Projection[2].AsExpr());

        sql = "SELECT STRUCT<NUMERIC>(NUMERIC '1'), STRUCT<BIGNUMERIC>(BIGNUMERIC '1')";
        select = VerifiedOnlySelect(sql);
        Assert.Equal(2, select.Projection.Count);

        Assert.Equal(new Struct([
            new TypedString("1", new DataType.Numeric(new ExactNumberInfo.None()))
        ], [
            new StructField(new DataType.Numeric(new ExactNumberInfo.None()))
        ]), select.Projection[0].AsExpr());

        Assert.Equal(new Struct([
            new TypedString("1", new DataType.BigNumeric(new ExactNumberInfo.None()))
        ], [
            new StructField(new DataType.BigNumeric(new ExactNumberInfo.None()))
        ]), select.Projection[1].AsExpr());
    }
}