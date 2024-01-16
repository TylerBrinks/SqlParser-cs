using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.Expression;

// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests.Dialects
{
    public class ClickHouseDialectTests : ParserTestBase
    {
        public ClickHouseDialectTests()
        {
            DefaultDialects = new[]{ new ClickHouseDialect()};
        }

        [Fact]
        public void Parse_Map_Access_Expr()
        {
            var select =
                VerifiedOnlySelect("""
                    SELECT string_values[indexOf(string_names, 'endpoint')]
                     FROM foos WHERE id = 'test'
                     AND string_value[indexOf(string_name, 'app')] <> 'foo'
                    """);

            var expected = new Select(new []
            {
                new SelectItem.UnnamedExpression(new MapAccess(new Identifier("string_values"), new Expression[]
                {
                    new Function("indexOf")
                    {
                        Args = new []
                        {
                            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("string_names"))),
                            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.SingleQuotedString("endpoint"))))
                        }
                    }
                }))
            })
            {
                From = new TableWithJoins[]
                {
                    new (new TableFactor.Table("foos"))
                },
                Selection = new BinaryOp(
                    new BinaryOp(
                        new Identifier("id"),
                       BinaryOperator.Eq,
                        new LiteralValue(new Value.SingleQuotedString("test"))
                    ),
                   BinaryOperator.And,
                    new BinaryOp(
                        new MapAccess(new Identifier("string_value"), new Expression[]
                        {
                            new Function("indexOf")
                            {
                                Args = new []
                                {
                                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("string_name"))),
                                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.SingleQuotedString("app"))))
                                }
                            }
                        }),
                        BinaryOperator.NotEq,
                        new LiteralValue(new Value.SingleQuotedString("foo"))
                    )
                )
            };

            Assert.Equal(expected, select);
        }

        [Fact]
        public void Parse_Array_Expr()
        {
            var select = VerifiedOnlySelect("SELECT ['1', '2'] FROM test");
            var expected = new Expression.Array(new ArrayExpression(new []
            {
                new LiteralValue(new Value.SingleQuotedString("1")),
                new LiteralValue(new Value.SingleQuotedString("2")),
            }));

            Assert.Equal(expected, select.Projection.Single().AsExpr());
        }

        [Fact]
        public void Parse_Array_Fn()
        {
            var select = VerifiedOnlySelect("SELECT array(x1, x2) FROM foo");
            var expected = new Function("array")
            {
                Args = new []
                {
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("x1"))),
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("x2"))),
                }
            };
            Assert.Equal(expected, select.Projection.Single().AsExpr());
        }

        [Fact]
        public void Parse_Kill()
        {
            var kill = VerifiedStatement<Statement.Kill>("KILL MUTATION 5");
            Assert.Equal(new Statement.Kill(KillType.Mutation, 5), kill);
        }

        [Fact]
        public void Parse_Delimited_Identifiers()
        {
            var select = VerifiedOnlySelect("SELECT \"alias\".\"bar baz\", \"myfun\"(), \"simple id\" AS \"column alias\" FROM \"a table\" AS \"alias\"");
            var table = (TableFactor.Table) select.From!.Single().Relation!;
            Assert.Equal(new ObjectName(new Ident("a table", Symbols.DoubleQuote)), table.Name);
            Assert.Equal(new Ident("alias", Symbols.DoubleQuote), table.Alias!.Name);
            Assert.Equal(3, select.Projection.Count);
            Assert.Equal(new CompoundIdentifier(new Ident[]
            {
                new ("alias", Symbols.DoubleQuote),
                new ("bar baz", Symbols.DoubleQuote)
            }), select.Projection[0].AsExpr());

            Assert.Equal(new Function(new ObjectName(new Ident("myfun", Symbols.DoubleQuote))), select.Projection[1].AsExpr());

            var expr = (SelectItem.ExpressionWithAlias)select.Projection[2];
            Assert.Equal(new Identifier(new Ident("simple id", Symbols.DoubleQuote)), expr.Expression);
            Assert.Equal(new Ident("column alias", Symbols.DoubleQuote), expr.Alias);

            VerifiedStatement("CREATE TABLE \"foo\" (\"bar\" \"int\")");
            VerifiedStatement("ALTER TABLE foo ADD CONSTRAINT \"bar\" PRIMARY KEY (baz)");
        }

        [Fact]
        public void Parse_Like()
        {
            Test(false);
            Test(true);

            void Test(bool negated)
            {
                var negation = negated ? "NOT " : null;
                var sql = $"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a'";
                var select = VerifiedOnlySelect(sql);

                var expected = new SimilarTo(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")));

                Assert.Equal(expected, select.Selection);

                // Test with escape char
                sql = $"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a' ESCAPE '\\'";
                select = VerifiedOnlySelect(sql);

                expected = new SimilarTo(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")), Symbols.Backslash);
                Assert.Equal(expected, select.Selection);

                sql = $"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a' ESCAPE '\\' IS NULL";
                select = VerifiedOnlySelect(sql);
                var isNull = new IsNull( new SimilarTo(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")), Symbols.Backslash) );
                Assert.Equal(isNull, select.Selection);
            }
        }

        [Fact]
        public void Parse_Create_Tables()
        {
            VerifiedStatement("CREATE TABLE \"x\" (\"a\" \"int\") ENGINE=MergeTree ORDER BY (\"x\")");

            OneStatementParsesTo(
                "CREATE TABLE \"x\" (\"a\" \"int\") ENGINE=MergeTree ORDER BY \"x\"",
                "CREATE TABLE \"x\" (\"a\" \"int\") ENGINE=MergeTree ORDER BY (\"x\")");

            VerifiedStatement("CREATE TABLE \"x\" (\"a\" \"int\") ENGINE=MergeTree ORDER BY (\"x\") AS SELECT * FROM \"t\" WHERE true");
        }

        [Fact]
        public void Parse_Double_Equal()
        {
            OneStatementParsesTo(
                "SELECT foo FROM bar WHERE buz == 'buz'",
                "SELECT foo FROM bar WHERE buz = 'buz'"
            );
        }
    }
}
