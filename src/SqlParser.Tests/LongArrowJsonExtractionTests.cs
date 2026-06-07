using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Expression;

namespace SqlParser.Tests;

// Regression tests for https://github.com/TylerBrinks/SqlParser-cs/issues/70
//
// The `->>` (LongArrow) JSON-extraction operator was incorrectly consuming the
// character immediately following `->>` when there was no whitespace between
// the operator and the next token. When that character was a single quote
// starting a string literal, the tokenizer would then run past the closing
// quote looking for a terminator that no longer existed, raising
// "Unterminated string literal". Example from the bug report:
//
//     meta->>'description'       -- failed to tokenize
//     meta ->> 'description'     -- worked because the extra Next() ate the space
//
// Scope of these tests
// --------------------
// `->>` is NOT part of the ANSI/ISO SQL standard. It originated as a
// PostgreSQL extension and has since been adopted by a handful of other
// engines. It is NOT supported by Snowflake, BigQuery, MS SQL Server, Hive,
// Databricks, ClickHouse, Oracle, or ANSI SQL, which use other mechanisms for
// JSON extraction (`JSON_VALUE`, `get_json_object`, `col:path`, etc.).
//
// We therefore only exercise these tests against dialects that genuinely
// support `->>`:
//   - PostgreSQL  (native, originated here)
//   - DuckDB      (the dialect from the bug report)
//   - MySQL       (since 5.7.13, as shorthand for JSON_UNQUOTE(JSON_EXTRACT(...)))
//   - SQLite      (since 3.38.0, 2022)
//   - Redshift    (Postgres-derived)
//   - Generic     (the repo's permissive catch-all dialect)
public class LongArrowJsonExtractionTests : ParserTestBase
{
    private static readonly Dialect[] DialectsSupportingLongArrow =
    [
        new PostgreSqlDialect(),
        new DuckDbDialect(),
        new MySqlDialect(),
        new SQLiteDialect(),
        new RedshiftDialect(),
        new GenericDialect()
    ];

    public static readonly IEnumerable<object[]> LongArrowDialects =
        DialectsSupportingLongArrow.Select(d => new object[] { d });

    [Theory]
    [MemberData(nameof(LongArrowDialects))]
    public void Tokenize_LongArrow_Followed_By_String_Literal(Dialect dialect)
    {
        var tokens = new Tokenizer().Tokenize("a->>'x'", dialect);

        var expected = new Token[]
        {
            new Word("a"),
            new LongArrow(),
            new SingleQuotedString("x")
        };

        TokenizerTestBase.Compare(expected, tokens);
    }

    [Theory]
    [MemberData(nameof(LongArrowDialects))]
    public void Parse_LongArrow_With_No_Whitespace_Before_String(Dialect dialect)
    {
        var parsed = new Parser().ParseSql("SELECT meta->>'description' FROM events", dialect);

        Assert.Single(parsed);
        var select = ((SetExpression.SelectExpression)parsed[0]!.AsQuery()!.Body).Select;

        var expected = new SelectItem.UnnamedExpression(new BinaryOp(
            new Identifier("meta"),
            BinaryOperator.LongArrow,
            new LiteralValue(new Value.SingleQuotedString("description"))
        ));

        Assert.Equal(expected, select.Projection.Single());
    }

    [Fact]
    public void Parse_Issue_70_Repro()
    {
        // Exact SQL from the bug report.
        const string sql = """
                           select
                           category_seq as seq,
                           data.name as name,
                           meta->>'description' as description
                           from category
                           order by seq
                           """;

        var parsed = new Parser().ParseSql(sql, new DuckDbDialect());
        Assert.Single(parsed);
    }
}
