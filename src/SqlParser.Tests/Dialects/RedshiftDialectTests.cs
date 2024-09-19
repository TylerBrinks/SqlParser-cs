using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.Expression;

// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests.Dialects;

public class RedshiftDialectTests : ParserTestBase
{
    public RedshiftDialectTests()
    {
        DefaultDialects = new[] { new RedshiftDialect() };
    }

    [Fact]
    public void Test_Square_Brackets_Over_Db_Schema_Table_Name()
    {
        var select = VerifiedOnlySelect("SELECT [col1] FROM [test_schema].[test_table]");

        var expected = new SelectItem.UnnamedExpression(new Identifier(new Ident("col1", Symbols.SquareBracketOpen)));

        Assert.Equal(expected, select.Projection[0]);

        var from = new TableWithJoins(new TableFactor.Table(new ObjectName(
        [
            new("test_schema", Symbols.SquareBracketOpen),
            new("test_table", Symbols.SquareBracketOpen)
        ])));

        Assert.Equal(from, select.From!.Single());
    }

    [Fact]
    public void Test_Double_Quotes_Over_Db_Schema_Table_Name()
    {
        var select = VerifiedOnlySelect("SELECT \"col1\" FROM \"test_schema\".\"test_table\"");

        var expected = new SelectItem.UnnamedExpression(new Identifier(new Ident("col1", Symbols.DoubleQuote)));

        Assert.Equal(expected, select.Projection[0]);

        var from = new TableWithJoins(new TableFactor.Table(new ObjectName(
        [
            new("test_schema", Symbols.DoubleQuote),
            new("test_table", Symbols.DoubleQuote)
        ])));

        Assert.Equal(from, select.From!.Single());
    }

    [Fact]
    public void Parse_Delimited_Identifiers()
    {
        var select = VerifiedOnlySelect("SELECT \"alias\".\"bar baz\", \"myfun\"(), \"simple id\" AS \"column alias\" FROM \"a table\" AS \"alias\"");

        var table = new TableFactor.Table(new ObjectName(new Ident("a table", Symbols.DoubleQuote)))
        {
            Alias = new TableAlias(new Ident("alias", Symbols.DoubleQuote))
        };

        Assert.Equal(table, select.From!.Single().Relation);

        Assert.Equal(3, select.Projection.Count);
        Assert.Equal(new CompoundIdentifier(new Ident[]
        {
            new("alias", Symbols.DoubleQuote),
            new("bar baz", Symbols.DoubleQuote)
        }), select.Projection[0].AsExpr());

        Assert.Equal(new Function(new ObjectName(new Ident("myfun", Symbols.DoubleQuote)))
        {
            Args = new FunctionArguments.List(FunctionArgumentList.Empty())
        }, select.Projection[1].AsExpr());

        var expr = new SelectItem.ExpressionWithAlias(new Identifier(new Ident("simple id", Symbols.DoubleQuote)),
            new Ident("column alias", Symbols.DoubleQuote));

        Assert.Equal(expr, select.Projection[2]);

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
    public void Test_Sharp()
    {
        var select = VerifiedOnlySelect("SELECT #_of_values");

        var expected = new SelectItem.UnnamedExpression(new Identifier("#_of_values"));
        Assert.Equal(expected, select.Projection[0]);
    }

    [Fact]
    public void Test_Create_View_With_No_Schema_Binding()
    {
        VerifiedStatement("CREATE VIEW myevent AS SELECT eventname FROM event WITH NO SCHEMA BINDING",
            [new RedshiftDialect(), new GenericDialect()]);
    }
}