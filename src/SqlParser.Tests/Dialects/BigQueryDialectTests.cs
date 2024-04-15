using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.Expression;
// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests.Dialects
{
    public class BigQueryDialectTests : ParserTestBase
    {
        public BigQueryDialectTests()
        {
            DefaultDialects = new[] { new BigQueryDialect() };
        }

        [Fact]
        public void Parse_Literal_String()
        {
            var select = VerifiedOnlySelect("SELECT 'single', \"double\"");
            Assert.Equal(2, select.Projection.Count);
            Assert.Equal(new LiteralValue(new Value.SingleQuotedString("single")), select.Projection.First().AsExpr());
            Assert.Equal(new LiteralValue(new Value.DoubleQuotedString("double")), select.Projection.Last().AsExpr());
        }

        [Fact]
        public void Parse_Byte_Literal()
        {
            var select = VerifiedOnlySelect("SELECT B'abc', B\"abc\"");
            Assert.Equal(2, select.Projection.Count);
            Assert.Equal(new LiteralValue(new Value.SingleQuotedByteStringLiteral("abc")), select.Projection.First().AsExpr());
            Assert.Equal(new LiteralValue(new Value.DoubleQuotedByteStringLiteral("abc")), select.Projection.Last().AsExpr());

            OneStatementParsesTo("SELECT b'abc', b\"abc\"", "SELECT B'abc', B\"abc\"");
        }

        [Fact]
        public void Parse_Raw_Literal()
        {
            var query = OneStatementParsesTo<Statement.Select>(
                "SELECT R'abc', R\"abc\", R'f\\(abc,(.*),def\\)', R\"f\\(abc,(.*),def\\)\"",
                "SELECT R'abc', R'abc', R'f\\(abc,(.*),def\\)', R'f\\(abc,(.*),def\\)'");

            var body = (SetExpression.SelectExpression)query.Query.Body;
            var select = body.Select;

            Assert.Equal(4, select.Projection.Count);
            Assert.Equal(new LiteralValue(new Value.RawStringLiteral("abc")), select.Projection[0].AsExpr());
            Assert.Equal(new LiteralValue(new Value.RawStringLiteral("abc")), select.Projection[1].AsExpr());
            Assert.Equal(new LiteralValue(new Value.RawStringLiteral("f\\(abc,(.*),def\\)")), select.Projection[2].AsExpr());
            Assert.Equal(new LiteralValue(new Value.RawStringLiteral("f\\(abc,(.*),def\\)")), select.Projection[3].AsExpr());
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
                new (new TableFactor.UnNest(new Sequence<Expression>{ new CompoundIdentifier(new Ident[]{"t1","a"}) })
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
        public void Parse_Like()
        {
            Test(true);
            Test(false);

            void Test(bool negated)
            {
                var negation = negated ? "NOT " : null;

                var sql = $"SELECT * FROM customers WHERE name {negation}LIKE '%a'";
                var select = VerifiedOnlySelect(sql);
                var expected = new Like(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")));
                Assert.Equal(expected, select.Selection);

                // Test with escape char
                sql = $"SELECT * FROM customers WHERE name {negation}LIKE '%a' ESCAPE '\\'";
                select = VerifiedOnlySelect(sql);
                expected = new Like(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")))
                {
                    EscapeChar = '\\'
                };
                Assert.Equal(expected, select.Selection);

                // This statement tests that LIKE and NOT LIKE have the same precedence.
                // This was previously mishandled (#81).
                sql = $"SELECT * FROM customers WHERE name {negation}LIKE '%a' IS NULL";
                select = VerifiedOnlySelect(sql);
                var isNull = new IsNull(new Like(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a"))));
                Assert.Equal(isNull, select.Selection);
            }
        }

        [Fact]
        public void Parse_Similar_To()
        {
            Test(false);
            Test(true);

            void Test(bool negated)
            {
                var negation = negated ? "NOT " : null;

                var sql = $"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a'";

                var select = VerifiedOnlySelect(sql);
                var expected = new SimilarTo(
                    new Identifier("name"),
                    negated,
                    new LiteralValue(new Value.SingleQuotedString("%a")));
                Assert.Equal(expected, select.Selection);


                // Test with escape char
                sql = $"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a' ESCAPE '\\'";
                select = VerifiedOnlySelect(sql);
                expected = new SimilarTo(
                    new Identifier("name"),
                    negated,
                    new LiteralValue(new Value.SingleQuotedString("%a")),
                    '\\');
                Assert.Equal(expected, select.Selection);


                // This statement tests that SIMILAR TO and NOT SIMILAR TO have the same precedence.
                sql = $"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a' ESCAPE '\\' IS NULL";
                select = VerifiedOnlySelect(sql);
                var isNull = new IsNull(new SimilarTo(
                    new Identifier("name"),
                    negated,
                    new LiteralValue(new Value.SingleQuotedString("%a")),
                    '\\'));
                Assert.Equal(isNull, select.Selection);
            }
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
                "SELECT ARRAY_AGG(x ORDER BY x, y) AS a FROM T",
                "SELECT ARRAY_AGG(x ORDER BY x ASC, y DESC) AS a FROM T"
            })
            {
                VerifiedStatement(sql, supportedDialects);
            }

            var wildcardSql = "SELECT ARRAY_AGG(sections_tbl.*) AS sections FROM sections_tbl";
            foreach (var dialect in AllDialects)
            {
                if (dialect is PostgreSqlDialect) { continue; }

                Assert.Throws<ParserException>(() => ParseSqlStatements(wildcardSql));
            }
        }

        [Fact]
        public void Test_Select_Wildcard_With_Except()
        {
            DefaultDialects = new Dialect[] { new BigQueryDialect(), new GenericDialect() };
            var select = VerifiedOnlySelect("SELECT * EXCEPT (col_a) FROM data");
            var expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
            {
                ExceptOption = new ExceptSelectItem("col_a", new Ident[] { }),
            });
            Assert.Equal(expected, select.Projection[0]);


            select = VerifiedOnlySelect("SELECT * EXCEPT (department_id, employee_id) FROM employee_table");
            expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
            {
                ExceptOption = new ExceptSelectItem("department_id", new Ident[] { "employee_id" }),
            });
            Assert.Equal(expected, select.Projection[0]);

            var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * EXCEPT () FROM employee_table"));
            Assert.Equal("Expected identifier, found ), Line: 1, Col: 18", ex.Message);
        }

        [Fact]
        public void Test_Select_Wildcard_With_Replace()
        {
            var select = VerifiedOnlySelect("SELECT * REPLACE ('widget' AS item_name) FROM orders");
            var expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
            {
                ReplaceOption = new ReplaceSelectItem(new ReplaceSelectElement[]
                {
                    new(new LiteralValue(new Value.SingleQuotedString("widget")), "item_name", true)
                })
            });

            Assert.Equal(expected, select.Projection[0]);

            select = VerifiedOnlySelect("SELECT * REPLACE (quantity / 2 AS quantity, 3 AS order_id) FROM orders");
            expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
            {
                ReplaceOption = new ReplaceSelectItem(new ReplaceSelectElement[]
                {
                    new (new BinaryOp(
                            new Identifier("quantity"),
                            BinaryOperator.Divide,
                            new LiteralValue(Number("2"))
                        ), "quantity", true),

                    new(new LiteralValue(Number("3")), "order_id", true)
                })
            });

            Assert.Equal(expected, select.Projection[0]);
        }

        [Fact]
        public void Parse_Map_Access_Offset()
        {
            var select = VerifiedOnlySelect("SELECT d[offset(0)]");
            var expected = new SelectItem.UnnamedExpression(new MapAccess(
                new Identifier("d"), new[]
                {
                    new Function("offset")
                    {
                        Args = new []
                        {
                            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new LiteralValue(Number("0"))))
                        }
                    }
                }));

            Assert.Equal(expected, select.Projection[0]);

            foreach (var sql in new[]{
                "SELECT d[SAFE_OFFSET(0)]",
                "SELECT d[ORDINAL(0)]",
                "SELECT d[SAFE_ORDINAL(0)]"
            })
            {
                VerifiedOnlySelect(sql);
            }
        }

        [Fact]
        public void Parse_Table_Time_Travel()
        {
            var dialect = new[] { new BigQueryDialect() };
            var version = "2023-08-18 23:08:18";
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
                TrimCharacters: new Sequence<Expression>
                {
                    new LiteralValue(new Value.SingleQuotedString("a"))
                });

            Assert.Equal(expected, select.Projection.First().AsExpr());

            Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT TRIM('xyz' 'a')", new[] { new BigQueryDialect() }));
        }

        [Fact]
        public void Parse_Nested_Data_Type()
        {
            var sql = "CREATE TABLE table (x STRUCT<a ARRAY<INT64>, b BYTES(42)>, y ARRAY<STRUCT<INT64>>)";

            var create = (Statement.CreateTable)OneStatementParsesTo(sql, sql, new[] { new BigQueryDialect() });

            var columns = new Sequence<ColumnDef>
            {
                new ("x", new DataType.Struct(new Sequence<StructField>
                {
                    new (new DataType.Array(new ArrayElementTypeDef.AngleBracket(new DataType.Int64())), "a"),
                    new(new DataType.Bytes(42), "b")
                })),
                new("y", new DataType.Array(new ArrayElementTypeDef.AngleBracket(new DataType.Struct(new Sequence<StructField>
                {
                    new (new DataType.Int64())
                }))))
            };
            var expected = new Statement.CreateTable("table", columns);
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
            var sql = "SELECT (1, 2, 3), (1, 1.0, '123', true)";
            var select = VerifiedOnlySelect(sql);

            var expected = new Expression.Tuple(new Sequence<Expression>
            {
                new LiteralValue(new Value.Number("1")),
                new LiteralValue(new Value.Number("2")),
                new LiteralValue(new Value.Number("3")),
            });
            Assert.Equal(expected, select.Projection.First().AsExpr());

            expected = new Expression.Tuple(new Sequence<Expression>
            {
                new LiteralValue(new Value.Number("1")),
                new LiteralValue(new Value.Number("1.0")),
                new LiteralValue(new Value.SingleQuotedString("123")),
                new LiteralValue(new Value.Boolean(true)),
            });

            Assert.Equal(expected, select.Projection.Skip(1).First().AsExpr());
        }

        [Fact]
        public void Parse_Typeless_Struct_Syntax()
        {
            var sql = "SELECT STRUCT(1, 2, 3), STRUCT('abc'), STRUCT(1, t.str_col), STRUCT(1 AS a, 'abc' AS b), STRUCT(str_col AS abc)";
            var select = VerifiedOnlySelect(sql);

            Expression expected = new Struct(new Sequence<Expression>
            {
                new LiteralValue(new Value.Number("1")),
                new LiteralValue(new Value.Number("2")),
                new LiteralValue(new Value.Number("3"))
            }, new Sequence<StructField>());
            Assert.Equal(expected, select.Projection.First().AsExpr());

            expected = new Struct(new Sequence<Expression>
            {
                new LiteralValue(new Value.SingleQuotedString("abc")),
            }, new Sequence<StructField>());
            Assert.Equal(expected, select.Projection.Skip(1).First().AsExpr());

            expected = new Struct(new Sequence<Expression>
            {
                new LiteralValue(new Value.Number("1")),
                new CompoundIdentifier(new Sequence<Ident> {"t", "str_col"})
            }, new Sequence<StructField>());
            Assert.Equal(expected, select.Projection.Skip(2).First().AsExpr());

            expected = new Struct(new Sequence<Expression>
            {
               new Named(new LiteralValue(new Value.Number("1")), "a"),
               new Named(new LiteralValue(new Value.SingleQuotedString("abc")), "b")
            }, new Sequence<StructField>());
            Assert.Equal(expected, select.Projection.Skip(3).First().AsExpr());

            expected = new Struct(new Sequence<Expression>
            {
               new Named(new Identifier("str_col"), "abc")
            }, new Sequence<StructField>());
            Assert.Equal(expected, select.Projection.Skip(4).First().AsExpr());
        }

        [Fact]
        public void Parse_Delete_Statement()
        {
            var delete = (Statement.Delete)VerifiedStatement("DELETE \"table\" WHERE 1");
            var relation = (delete.From as FromTable.WithoutKeyword).From.First().Relation;

            var expected = new TableFactor.Table(new ObjectName(new Ident("table", Symbols.DoubleQuote)));

            Assert.Equal(expected, relation);
        }

        [Fact]
        public void Parse_Create_View_If_Not_Exists()
        {
            var sql = "CREATE VIEW IF NOT EXISTS mydataset.newview AS SELECT foo FROM bar";
            var create = (Statement.CreateView)VerifiedStatement(sql);


            Assert.Equal("mydataset.newview", create.Name);
            Assert.Equal("SELECT foo FROM bar", create.Query.ToSql());
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
            var create = VerifiedStatement< Statement.CreateView>("CREATE VIEW IF NOT EXISTS my-pro-ject.mydataset.myview AS SELECT 1");
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

            Assert.Equal(name, create.Name);
            Assert.Equal(columns, create.Columns);
        }

        [Fact]
        public void Parse_Exact_Weekday()
        {
            var select = VerifiedOnlySelect("SELECT EXTRACT(WEEK(MONDAY) FROM d)");

            var expected = new Extract(new Identifier("d"), new DateTimeField.Week("MONDAY"));

            Assert.Equal(expected, select.Projection.First().AsExpr());
        }
    }
}
