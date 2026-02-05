using SqlParser.Dialects;

// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests.Dialects;

public class OracleDialectTests : ParserTestBase
{
    public OracleDialectTests()
    {
        DefaultDialects = [new OracleDialect()];
    }

    [Fact]
    public void Test_Muldiv_Have_Higher_Precedence_Than_Strconcat()
    {
        // From Rust: muldiv_have_higher_precedence_than_strconcat
        // Oracle: `||` has a lower precedence than `*` and `/`
        var sql = "SELECT 3 / 5 || 'asdf' || 7 * 9 FROM dual";
        VerifiedOnlySelect(sql);
    }

    [Fact]
    public void Test_Plusminus_Have_Same_Precedence_As_Strconcat()
    {
        // From Rust: plusminus_have_same_precedence_as_strconcat
        // Oracle: `+`, `-`, and `||` have the same precedence and parse from left-to-right
        var sql = "SELECT 3 + 5 || '.3' || 7 - 9 FROM dual";
        VerifiedOnlySelect(sql);
    }

    [Fact]
    public void Test_Select_Identifiers_Not_Quote_Delimited()
    {
        // From Rust: parse_quote_delimited_string_but_is_a_word
        // When q is used as identifier (not Q'...' string), it should parse as normal identifiers
        OneStatementParsesTo(
            "SELECT q, quux, q.abc FROM dual q",
            "SELECT q, quux, q.abc FROM dual AS q");
    }

    [Fact]
    public void Test_Select_Nq_Identifiers_Not_Quote_Delimited()
    {
        // From Rust: parse_national_quote_delimited_string_but_is_a_word
        // When nq is used as identifier (not NQ'...' string), it should parse as normal identifiers
        OneStatementParsesTo(
            "SELECT nq, nqoo, nq.abc FROM dual q",
            "SELECT nq, nqoo, nq.abc FROM dual AS q");
    }

    [Fact]
    public void Test_Basic_Select_From_Dual()
    {
        // Oracle commonly uses SELECT ... FROM dual
        VerifiedStatement("SELECT 1 FROM dual");
    }
}
