using SqlParser.Dialects;

namespace SqlParser.Tests.Dialects;

public class DialectTests
{
    [Fact]
    public void Identifier_Quoted_Style()
    {
        Assert.Null(new GenericDialect().IdentifierQuoteStyle("id"));
        Assert.Equal(Symbols.Backtick, new SQLiteDialect().IdentifierQuoteStyle("id")!);
        Assert.Equal(Symbols.DoubleQuote, new PostgreSqlDialect().IdentifierQuoteStyle("id")!);
        Assert.Equal(Symbols.Backtick, new MySqlDialect().IdentifierQuoteStyle("id")!);
    }
}
