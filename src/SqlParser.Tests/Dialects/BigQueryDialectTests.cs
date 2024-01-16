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
            Test("da-sh-es", new Ident[] { "da-sh-es" });
            Test("`spa ce`", new Ident[] { new("spa ce", '`') });
            Test("`!@#$%^&*()-=_+`", new Ident[] { new("!@#$%^&*()-=_+", '`') });
            Test("_5abc.dataField", new Ident[] { "_5abc", "dataField" });
            Test("`5abc`.dataField", new Ident[] { new("5abc", '`'), "dataField" });
            Assert.Throws<ParserException>(() => VerifiedStatement("SELECT 1 FROM 5abc.dataField"));
            Test("abc5.dataField", new Ident[] { "abc5", "dataField" });
            Assert.Throws<ParserException>(() => VerifiedStatement("SELECT 1 FROM abc5!.dataField"));
            Test("`GROUP`.dataField", new Ident[] { new("GROUP", '`'), "dataField" });
            Test("abc5.GROUP", new Ident[] { "abc5", "GROUP" });
            Test("`foo.bar.baz`", new []
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
    }
}
