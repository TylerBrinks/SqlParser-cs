using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.Expression;

// ReSharper disable CommentTypo

namespace SqlParser.Tests.Dialects;

public class DatabricksDialectTests : ParserTestBase
{
    public DatabricksDialectTests()
    {
        DefaultDialects = [new DatabricksDialect()];
    }

    [Fact]
    public void Test_Databricks_Identifiers()
    {
        var select = VerifiedOnlySelect("SELECT `Ä`");
        Assert.Equal(new SelectItem.UnnamedExpression(new Identifier(new Ident("Ä", Symbols.Backtick))), select.Projection[0]);

        select = VerifiedOnlySelect("SELECT \"Ä\"");
        Assert.Equal(new SelectItem.UnnamedExpression(new LiteralValue(new Value.DoubleQuotedString("Ä"))), select.Projection[0]);
    }

    [Fact]
    public void Test_Databricks_Exists()
    {
        VerifiedExpr("exists(array(1, 2, 3), x -> x IS NULL)");
    }

    [Fact]
    public void Test_Values_Clause()
    {
        var query = VerifiedQuery("VALUES (\"one\", 1), ('two', 2)");

        var expected = new SetExpression.ValuesExpression(new Values([
            [
                new LiteralValue(new Value.DoubleQuotedString("one")),
                new LiteralValue(new Value.Number("1"))
            ],
            [
                new LiteralValue(new Value.SingleQuotedString("two")),
                new LiteralValue(new Value.Number("2"))
            ]
        ]));

        Assert.Equal(expected, query.Body);

        VerifiedQueryWithCanonical("SELECT * FROM VALUES (\"one\", 1), ('two', 2)", "SELECT * FROM (VALUES (\"one\", 1), ('two', 2))");


        var tableFactor = new TableFactor.Table("values");
        query = VerifiedQuery("WITH values AS (SELECT 42) SELECT * FROM values");

        Assert.Equal(tableFactor, query.Body.AsSelect().From![0].Relation);
    }

    [Fact]
    public void Parse_Use()
    {
        List<string> validObjectNames = ["mydb", "WAREHOUSE", "DEFAULT"];

        List<char> quoteStyles = [Symbols.DoubleQuote, Symbols.Backtick];

        foreach (var objectName in validObjectNames)
        {
            var useStatement = VerifiedStatement<Statement.Use>($"USE {objectName}");
            var expected = new Use.Object(new ObjectName(new Ident(objectName)));
            Assert.Equal(expected, useStatement.Name);

            foreach (var quote in quoteStyles)
            {
                useStatement = VerifiedStatement<Statement.Use>($"USE {quote}{objectName}{quote}");
                expected = new Use.Object(new ObjectName(new Ident(objectName, quote)));
                Assert.Equal(expected, useStatement.Name);
            }
        }

        foreach (var quote in quoteStyles)
        {
            var useStatement = VerifiedStatement<Statement.Use>($"USE CATALOG {quote}my_catalog{quote}");
            Use expected = new Use.Catalog(new ObjectName(new Ident("my_catalog", quote)));
            Assert.Equal(expected, useStatement.Name);

            useStatement = VerifiedStatement<Statement.Use>($"USE DATABASE {quote}my_catalog{quote}");
            expected = new Use.Database(new ObjectName(new Ident("my_catalog", quote)));
            Assert.Equal(expected, useStatement.Name);

            useStatement = VerifiedStatement<Statement.Use>($"USE SCHEMA {quote}my_catalog{quote}");
            expected = new Use.Schema(new ObjectName(new Ident("my_catalog", quote)));
            Assert.Equal(expected, useStatement.Name);
        }

        Assert.Equal(new Statement.Use(new Use.Catalog("my_catalog")), VerifiedStatement("USE CATALOG my_catalog"));
        Assert.Equal(new Statement.Use(new Use.Database("my_schema")), VerifiedStatement("USE DATABASE my_schema"));
        Assert.Equal(new Statement.Use(new Use.Schema("my_schema")), VerifiedStatement("USE SCHEMA my_schema"));

        List<string> invalidCases = ["USE SCHEMA", "USE DATABASE", "USE CATALOG"];

        foreach (var sql in invalidCases)
        {
            Assert.Throws<ParserException>(() => ParseSqlStatements(sql));
        }
    }

    [Fact]
    public void Data_Type_Timestamp_Ntz()
    {
        // Literal
        VerifiedExpr("TIMESTAMP_NTZ '2025-03-29T18:52:00'");
        // Cast
        VerifiedExpr("(created_at)::TIMESTAMP_NTZ");
        // Column definition
        VerifiedStatement("CREATE TABLE foo (x TIMESTAMP_NTZ)");
    }

    [Fact]
    public void Parse_Databricks_Struct_Function()
    {
        VerifiedOnlySelect("SELECT STRUCT(1, 'foo')");
        VerifiedOnlySelect("SELECT STRUCT(1 AS one, 'foo' AS foo, false)");
    }

    [Fact]
    public void Test_Databricks_Lambdas()
    {
        var sql = "SELECT array_sort(array('Hello', 'World'), (p1, p2) -> CASE WHEN p1 = p2 THEN 0 WHEN reverse(p1) < reverse(p2) THEN -1 ELSE 1 END)";
        VerifiedOnlySelect(sql);
        VerifiedExpr("map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2))");
        VerifiedExpr("transform(array(1, 2, 3), x -> x + 1)");
    }
}
