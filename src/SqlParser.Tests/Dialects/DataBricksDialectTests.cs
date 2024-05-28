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
    }
}
