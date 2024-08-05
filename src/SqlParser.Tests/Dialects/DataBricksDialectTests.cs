using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.Expression;

// ReSharper disable CommentTypo

namespace SqlParser.Tests.Dialects
{
    public class DatabricksDialectTests : ParserTestBase
    {
        public DatabricksDialectTests()
        {
            DefaultDialects = new[] {new DatabricksDialect()};
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
    }
}
