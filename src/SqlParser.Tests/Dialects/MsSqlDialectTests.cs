﻿using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.Expression;

// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests.Dialects
{
    public class MsSqlDialectTests : ParserTestBase
    {
        public MsSqlDialectTests()
        {
            DefaultDialects = new[] { new MsSqlDialect() };
        }

        [Fact]
        public void Parse_MsSql_Identifiers()
        {
            var select = VerifiedOnlySelect("SELECT @@version, _foo$123 FROM ##temp");
            Assert.Equal(new Identifier("@@version"), select.Projection[0].AsExpr());
            Assert.Equal(new Identifier("_foo$123"), select.Projection[1].AsExpr());
            Assert.Equal(2, select.Projection.Count);
            var table = (TableFactor.Table) select.From!.Single().Relation!;
            Assert.Equal("##temp", table.Name);
        }


        [Fact]
        public void Parse_MsSql_Single_Quoted_Identifiers()
        {
            DefaultDialects = new Dialect[] { new MsSqlDialect(), new GenericDialect() };

            OneStatementParsesTo("SELECT foo 'alias'", "SELECT foo AS 'alias'");
        }

        [Fact]
        public void Parse_MsSql_Delimited_Identifiers()
        {
            OneStatementParsesTo(
                "SELECT [a.b!] [FROM] FROM foo [WHERE]",
                "SELECT [a.b!] AS [FROM] FROM foo AS [WHERE]");
        }

        [Fact]
        public void Parse_MsSql_Top_Paren()
        {
            DefaultDialects = new Dialect[] { new MsSqlDialect(), new GenericDialect() };

            var select = VerifiedOnlySelect("SELECT TOP (5) * FROM foo");
            Assert.Equal(new TopQuantity.TopExpression(new LiteralValue(new Value.Number("5"))), select.Top!.Quantity);
            Assert.False(select.Top!.Percent);
        }

        [Fact]
        public void Parse_MsSql_Top_Percent()
        {
            DefaultDialects = new Dialect[] { new MsSqlDialect(), new GenericDialect() };

            var select = VerifiedOnlySelect("SELECT TOP (5) PERCENT * FROM foo");
            Assert.Equal(new TopQuantity.TopExpression(new LiteralValue(new Value.Number("5"))), select.Top!.Quantity);
            Assert.True(select.Top!.Percent);
        }

        [Fact]
        public void Parse_MsSql_Top_With_Ties()
        {
            DefaultDialects = new Dialect[] { new MsSqlDialect(), new GenericDialect() };

            var select = VerifiedOnlySelect("SELECT TOP (10) PERCENT WITH TIES * FROM foo");
            Assert.Equal(new TopQuantity.TopExpression(new LiteralValue(new Value.Number("10"))), select.Top!.Quantity);
            Assert.True(select.Top!.Percent);
        }

        [Fact]
        public void Parse_MsSql_Top()
        {
            DefaultDialects = new Dialect[] { new MsSqlDialect(), new GenericDialect() };

            OneStatementParsesTo("SELECT TOP 5 bar, baz FROM foo", "SELECT TOP 5 bar, baz FROM foo");
        }

        [Fact]
        public void Parse_MsSql_Bin_Literal()
        {
            DefaultDialects = new Dialect[] { new MsSqlDialect(), new GenericDialect() };

            OneStatementParsesTo("SELECT 0xdeadBEEF", "SELECT X'deadBEEF'");
        }

        [Fact]
        public void Parse_MsSql_Create_Role()
        {
            var role = VerifiedStatement<Statement.CreateRole>("CREATE ROLE mssql AUTHORIZATION helena");

            Assert.Equal(new ObjectName[] { new("mssql") }, role.Names);
            Assert.Equal(new ObjectName("helena") ,role.AuthorizationOwner);
        }

        [Fact]
        public void Parse_Delimited_Identifiers()
        {
            var select = VerifiedOnlySelect("SELECT \"alias\".\"bar baz\", \"myfun\"(), \"simple id\" AS \"column alias\" FROM \"a table\" AS \"alias\"");

            var table = (TableFactor.Table) select.From!.Single().Relation!;

            Assert.Equal(new Ident[] { new("a table", Symbols.DoubleQuote) }, table.Name.Values);
            Assert.Equal(new Ident("alias", Symbols.DoubleQuote), table.Alias!.Name);

            Assert.Equal(3, select.Projection.Count);
            Assert.Equal(new CompoundIdentifier(new Ident[]
            {
                new("alias", Symbols.DoubleQuote),
                new("bar baz", Symbols.DoubleQuote)
            }), select.Projection[0].AsExpr());

            Assert.Equal(new Function(new ObjectName(new Ident("myfun", Symbols.DoubleQuote))), 
                select.Projection[1].AsExpr());

            var withAlias = (SelectItem.ExpressionWithAlias) select.Projection[2];

            Assert.Equal(new Identifier(new Ident("simple id", Symbols.DoubleQuote)), withAlias.Expression);
            Assert.Equal(new Ident("column alias", Symbols.DoubleQuote), withAlias.Alias);
        }

        [Fact]
        public void Parse_Like()
        {
            Test(false);
            Test(true);

            void Test(bool negated)
            {
                var negation = negated ? "NOT " : null;

                var select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}LIKE '%a'");
                var expected = new Like(new Identifier("name"), negated,
                    new LiteralValue(new Value.SingleQuotedString("%a")));
                Assert.Equal(expected, select.Selection);

                select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}LIKE '%a' ESCAPE '\\'");
                expected = new Like(new Identifier("name"), negated,
                    new LiteralValue(new Value.SingleQuotedString("%a")), Symbols.Backslash);
                Assert.Equal(expected, select.Selection);

                // This statement tests that LIKE and NOT LIKE have the same precedence.
                select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}LIKE '%a' IS NULL");
                var isNull = new IsNull(new Like(new Identifier("name"), negated,
                    new LiteralValue(new Value.SingleQuotedString("%a"))));
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

                var select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a'");
                var expected = new SimilarTo(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")));
                Assert.Equal(expected, select.Selection);

                select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a' ESCAPE '\\'");
                expected = new SimilarTo(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")), Symbols.Backslash);
                Assert.Equal(expected, select.Selection);

                // This statement tests that LIKE and NOT LIKE have the same precedence.
                select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a' ESCAPE '\\' IS NULL");
                var isNull = new IsNull(new SimilarTo(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")), Symbols.Backslash));
                Assert.Equal(isNull, select.Selection);
            }
        }

        [Fact]
        public void Parse_Create_Procedure()
        {
            const string sql = "CREATE OR ALTER PROCEDURE test (@foo INT, @bar VARCHAR(256)) AS BEGIN SELECT 1 END";

            var one = new Value.Number("1");

            var create = VerifiedStatement(sql);

            var select = new Select(new Sequence<SelectItem>
            {
                new SelectItem.UnnamedExpression(new LiteralValue(one))
            });

            var selectExpr = new SetExpression.SelectExpression(select);
            var query = new Query(selectExpr);

            var parameters = new Sequence<ProcedureParam>
            {
                new ("@foo", new DataType.Int()),
                new ("@bar", new DataType.Varchar(new CharacterLength.IntegerLength(256))),
            };

            var expected = new Statement.CreateProcedure(true, "test", parameters, new Sequence<Statement>{query});

            Assert.Equal(expected, create);
        }

        [Fact]
        public void Parse_Table_Name_In_Square_Brackets()
        {
            var select = VerifiedOnlySelect("SELECT [a column] FROM [a schema].[a table]");

            var table = (TableFactor.Table)select.From!.Single().Relation!;

            Assert.Equal(new ObjectName(new []
            {
                new Ident("a schema", '['),
                new Ident("a table", '['),
            } ), table.Name);

            Assert.Equal(new Identifier(new Ident("a column", '[')), select.Projection.First().AsExpr());
        }

        [Fact]
        public void Parse_Cast_Varchar_Max()
        {
            VerifiedExpr("CAST('foo' AS VARCHAR(MAX))", new Dialect[]{new MsSqlDialect(), new GenericDialect()});
        }

        [Fact]
        public void Parse_For_Clause()
        {
            var dialects = new Dialect[] {new MsSqlDialect(), new GenericDialect()};
            VerifiedStatement("SELECT a FROM t FOR JSON PATH", dialects);
            VerifiedStatement("SELECT b FROM t FOR JSON AUTO", dialects);
            VerifiedStatement("SELECT c FROM t FOR JSON AUTO, WITHOUT_ARRAY_WRAPPER", dialects);
            VerifiedStatement("SELECT 1 FROM t FOR JSON PATH, ROOT('x'), INCLUDE_NULL_VALUES", dialects);
            VerifiedStatement("SELECT 2 FROM t FOR XML AUTO", dialects);
            VerifiedStatement("SELECT 3 FROM t FOR XML AUTO, TYPE, ELEMENTS", dialects);
            VerifiedStatement("SELECT * FROM t WHERE x FOR XML AUTO, ELEMENTS", dialects);
            VerifiedStatement("SELECT x FROM t ORDER BY y FOR XML AUTO, ELEMENTS", dialects);
            VerifiedStatement("SELECT y FROM t FOR XML PATH('x'), ROOT('y'), ELEMENTS", dialects);
            VerifiedStatement("SELECT z FROM t FOR XML EXPLICIT, BINARY BASE64", dialects);
            VerifiedStatement("SELECT * FROM t FOR XML RAW('x')", dialects);
            VerifiedStatement("SELECT * FROM t FOR BROWSE", dialects);
        }

        [Fact]
        public void Dont_Parse_Trailing_For()
        {
            new []{ new MsSqlDialect() }.RunParserMethod("SELECT * FROM foo FOR", parser =>
            {
                Assert.Throws<ParserException>(() => parser.ParseQuery());
            });
        }

        [Fact]
        public void Parse_For_Json_Expect_Ast()
        {
            var query = VerifiedQuery("SELECT * FROM t FOR JSON PATH, ROOT('root')");

            var expected = new ForClause.Json(new ForJson.Path(), "root", false, false);
            
            Assert.Equal(expected, query.ForClause);
        }

        [Fact]
        public void Parse_Convert()
        {
            VerifiedExpr("CONVERT(VARCHAR(MAX), 'foo')");
            VerifiedExpr("CONVERT(VARCHAR(10), 'foo')");
            VerifiedExpr("CONVERT(DECIMAL(10,5), 12.55)");
        }
    }
}