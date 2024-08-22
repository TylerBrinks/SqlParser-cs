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
            var table = (TableFactor.Table)select.From!.Single().Relation!;
            Assert.Equal("##temp", table.Name);
        }

        [Fact]
        public void Parse_MsSql_Single_Quoted_Identifiers()
        {
            DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

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
            DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

            var select = VerifiedOnlySelect("SELECT TOP (5) * FROM foo");
            Assert.Equal(new TopQuantity.TopExpression(new LiteralValue(new Value.Number("5"))), select.Top!.Quantity);
            Assert.False(select.Top!.Percent);
        }

        [Fact]
        public void Parse_MsSql_Top_Percent()
        {
            DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

            var select = VerifiedOnlySelect("SELECT TOP (5) PERCENT * FROM foo");
            Assert.Equal(new TopQuantity.TopExpression(new LiteralValue(new Value.Number("5"))), select.Top!.Quantity);
            Assert.True(select.Top!.Percent);
        }

        [Fact]
        public void Parse_MsSql_Top_With_Ties()
        {
            DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

            var select = VerifiedOnlySelect("SELECT TOP (10) PERCENT WITH TIES * FROM foo");
            Assert.Equal(new TopQuantity.TopExpression(new LiteralValue(new Value.Number("10"))), select.Top!.Quantity);
            Assert.True(select.Top!.Percent);
        }

        [Fact]
        public void Parse_MsSql_Top()
        {
            DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

            OneStatementParsesTo("SELECT TOP 5 bar, baz FROM foo", "SELECT TOP 5 bar, baz FROM foo");
        }

        [Fact]
        public void Parse_MsSql_Output_Merge()
        {
            var mergeInto = VerifiedStatement<Statement.Merge>("""
                MERGE INTO s.bar AS dest USING (SELECT * FROM s.foo) AS stg ON dest.D = stg.D AND dest.E = stg.E 
                WHEN NOT MATCHED THEN INSERT (A, B, C) VALUES (stg.A, stg.B, stg.C) 
                WHEN MATCHED AND dest.A = 'a' THEN UPDATE SET dest.F = stg.F, dest.G = stg.G 
                WHEN MATCHED THEN DELETE 
                OUTPUT $action, inserted.*, deleted.*
                """);

            Assert.Equal([
                new SelectItem.UnnamedExpression(new LiteralValue(new Value.Placeholder("$action"))),
                new SelectItem.QualifiedWildcard(new ObjectName(new Ident("inserted")), new WildcardAdditionalOptions()),
                new SelectItem.QualifiedWildcard(new ObjectName(new Ident("deleted")), new WildcardAdditionalOptions()),
            ], mergeInto.Output);
        }

        [Fact]
        public void Parse_MsSql_Output_Insert()
        {
            var insert = VerifiedStatement<Statement.Insert>("""
                                                             INSERT INTO foo (id) 
                                                             OUTPUT INSERTED.* 
                                                             VALUES (1, 2, 3)
                                                             """);

            Assert.Equal([
                new SelectItem.QualifiedWildcard(new ObjectName(new Ident("INSERTED")), new WildcardAdditionalOptions()),
            ], insert.InsertOperation.Output);
        }

        [Fact]
        public void Parse_MsSql_Output_Delete()
        {
            var delete = VerifiedStatement<Statement.Delete>("""
                                                             DELETE FROM users 
                                                             OUTPUT deleted.id, deleted.name 
                                                             WHERE age > 30
                                                             """);

            Assert.Equal([
                new SelectItem.UnnamedExpression(new CompoundIdentifier(new Sequence<Ident>{new Ident("deleted"), new Ident("id")})),
                new SelectItem.UnnamedExpression(new CompoundIdentifier(new Sequence<Ident>{new Ident("deleted"), new Ident("name")})),
            ], delete.DeleteOperation.Output);

            delete = VerifiedStatement<Statement.Delete>("""
                                                             DELETE u 
                                                             OUTPUT deleted.id, deleted.name 
                                                             FROM users 
                                                             WHERE age > 30
                                                             """);

            Assert.Equal([
                new SelectItem.UnnamedExpression(new CompoundIdentifier(new Sequence<Ident>{new Ident("deleted"), new Ident("id")})),
                new SelectItem.UnnamedExpression(new CompoundIdentifier(new Sequence<Ident>{new Ident("deleted"), new Ident("name")})),
            ], delete.DeleteOperation.Output);
        }

        [Fact]
        public void Parse_MsSql_Output_Update()
        {
            var delete = VerifiedStatement<Statement.Update>("""
                                                             UPDATE users 
                                                             SET age = age + 1 
                                                             OUTPUT deleted.age AS OldAge, inserted.age AS NewAge 
                                                             WHERE name = 'Alice'
                                                             """);

            Assert.Equal([
                new SelectItem.ExpressionWithAlias(new CompoundIdentifier(new Ident[] { "deleted", "age" }),
                    new Ident("OldAge")),
                new SelectItem.ExpressionWithAlias(new CompoundIdentifier(new Ident[] { "inserted", "age" }),
                    new Ident("NewAge")),
            ], delete.Output);

            delete = VerifiedStatement<Statement.Update>("""
                                                             UPDATE u 
                                                             SET age = age + 1 
                                                             OUTPUT deleted.age AS OldAge, inserted.age AS NewAge 
                                                             FROM users 
                                                             WHERE name = 'Alice'
                                                             """);

            Assert.Equal([
                new SelectItem.ExpressionWithAlias(new CompoundIdentifier(new Ident[] { "deleted", "age" }),
                    new Ident("OldAge")),
                new SelectItem.ExpressionWithAlias(new CompoundIdentifier(new Ident[] { "inserted", "age" }),
                    new Ident("NewAge")),
            ], delete.Output);
        }

        [Fact]
        public void Parse_MsSql_Bin_Literal()
        {
            DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

            OneStatementParsesTo("SELECT 0xdeadBEEF", "SELECT X'deadBEEF'");
        }

        [Fact]
        public void Parse_MsSql_Create_Role()
        {
            var role = VerifiedStatement<Statement.CreateRole>("CREATE ROLE mssql AUTHORIZATION helena");

            Assert.Equal(new ObjectName[] { new("mssql") }, role.Names);
            Assert.Equal(new ObjectName("helena"), role.AuthorizationOwner);
        }

        [Fact]
        public void Parse_Delimited_Identifiers()
        {
            var select = VerifiedOnlySelect("SELECT \"alias\".\"bar baz\", \"myfun\"(), \"simple id\" AS \"column alias\" FROM \"a table\" AS \"alias\"");

            var table = (TableFactor.Table)select.From!.Single().Relation!;

            Assert.Equal(new Ident[] { new("a table", Symbols.DoubleQuote) }, table.Name.Values);
            Assert.Equal(new Ident("alias", Symbols.DoubleQuote), table.Alias!.Name);

            Assert.Equal(3, select.Projection.Count);
            Assert.Equal(new CompoundIdentifier(new Ident[]
            {
                new("alias", Symbols.DoubleQuote),
                new("bar baz", Symbols.DoubleQuote)
            }), select.Projection[0].AsExpr());

            Assert.Equal(new Function(new ObjectName(new Ident("myfun", Symbols.DoubleQuote)))
            {
                Args = new FunctionArguments.List(FunctionArgumentList.Empty())
            },
                select.Projection[1].AsExpr());

            var withAlias = (SelectItem.ExpressionWithAlias)select.Projection[2];

            Assert.Equal(new Identifier(new Ident("simple id", Symbols.DoubleQuote)), withAlias.Expression);
            Assert.Equal(new Ident("column alias", Symbols.DoubleQuote), withAlias.Alias);
        }

        [Fact]
        public void Parse_Create_Procedure()
        {
            const string sql = "CREATE OR ALTER PROCEDURE test (@foo INT, @bar VARCHAR(256)) AS BEGIN SELECT 1 END";

            var one = new Value.Number("1");

            var create = VerifiedStatement(sql);

            var select = new Select(
            [
                new SelectItem.UnnamedExpression(new LiteralValue(one))
            ]);

            var selectExpr = new SetExpression.SelectExpression(select);
            var query = new Query(selectExpr);

            var parameters = new Sequence<ProcedureParam>
            {
                new ("@foo", new DataType.Int()),
                new ("@bar", new DataType.Varchar(new CharacterLength.IntegerLength(256))),
            };

            var expected = new Statement.CreateProcedure(true, "test", parameters, [query]);

            Assert.Equal(expected, create);
        }

        [Fact]
        public void Parse_Table_Name_In_Square_Brackets()
        {
            var select = VerifiedOnlySelect("SELECT [a column] FROM [a schema].[a table]");

            var table = (TableFactor.Table)select.From!.Single().Relation!;

            Assert.Equal(new ObjectName(
            [
                new Ident("a schema", '['),
                new Ident("a table", '['),
            ]), table.Name);

            Assert.Equal(new Identifier(new Ident("a column", '[')), select.Projection.First().AsExpr());
        }

        [Fact]
        public void Parse_Cast_Varchar_Max()
        {
            VerifiedExpr("CAST('foo' AS VARCHAR(MAX))", [new MsSqlDialect(), new GenericDialect()]);
            VerifiedExpr("CAST('foo' AS NVARCHAR(MAX))", [new MsSqlDialect(), new GenericDialect()]);
        }

        [Fact]
        public void Parse_For_Clause()
        {
            var dialects = new Dialect[] { new MsSqlDialect(), new GenericDialect() };
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
            new[] { new MsSqlDialect() }.RunParserMethod("SELECT * FROM foo FOR", parser =>
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
            const string sql = "CONVERT(INT, 1, 2, 3, NULL)";
            var convert = (Expression.Convert)VerifiedExpr(sql);

            Assert.Equal(new LiteralValue(new Value.Number("1")), convert.Expression);
            Assert.Equal(new DataType.Int(), convert.DataType);
            Assert.Null(convert.CharacterSet);
            Assert.True(convert.TargetBeforeValue);

            Assert.Equal([
                new LiteralValue(new Value.Number("2")),
                new LiteralValue(new Value.Number("3")),
                new LiteralValue(new Value.Null())
            ], convert.Styles);

            VerifiedExpr("CONVERT(VARCHAR(MAX), 'foo')");
            VerifiedExpr("CONVERT(VARCHAR(10), 'foo')");
            VerifiedExpr("CONVERT(DECIMAL(10,5), 12.55)");

            Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT CONVERT(INT, 'foo',) FROM T"));
        }

        [Fact]
        public void Parse_MsSql_Declare()
        {
            var statement = ParseSqlStatements("DECLARE @foo CURSOR, @bar INT, @baz AS TEXT = 'foobar';")[0];

            var expected = new Statement.Declare([

                new Declare(["@foo"], null, null, DeclareType.Cursor),
                new Declare(["@bar"], new DataType.Int(), null, null),
                new Declare(["@baz"], new DataType.Text(),
                    new DeclareAssignment.MsSqlAssignment(new LiteralValue(new Value.SingleQuotedString("foobar"))), null)
            ]);

            Assert.Equal(expected, statement);
        }

        [Fact]
        public void Parse_Ampersand_Arobase()
        {
            ExpressionParsesTo("a&@b", "a & @b", new List<Dialect> { new MsSqlDialect() });
        }
    }
}
