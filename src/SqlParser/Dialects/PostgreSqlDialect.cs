using SqlParser.Ast;
using SqlParser.Tokens;

namespace SqlParser.Dialects;

/// <summary>
/// PostgreSql dialect
///
/// <see href="https://www.postgresql.org/docs/current/sql-syntax.html"/>
/// </summary>
public class PostgreSqlDialect : Dialect
{
    // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    // We don't yet support identifiers beginning with "letters with
    public override bool IsIdentifierStart(char character)
    {
        // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        // We don't yet support identifiers beginning with "letters with
        // diacritical marks and non-Latin letters"
        return char.IsLetter(character) || character is Symbols.Underscore;
    }

    public override bool IsIdentifierPart(char character)
    {
        return char.IsLetterOrDigit(character) || character is Symbols.Dollar or Symbols.Underscore;
    }

    public override bool SupportsFilterDuringAggregation => true;

    public override char? IdentifierQuoteStyle(string identifier) => Symbols.DoubleQuote;

    public override bool IsDelimitedIdentifierStart(char character)=> character is Symbols.DoubleQuote;

    public override bool IsCustomOperatorPart(char character) => character
            is Symbols.Plus
            or Symbols.Minus
            or Symbols.Asterisk
            or Symbols.Divide
            or Symbols.LessThan
            or Symbols.GreaterThan
            or Symbols.Equal
            or Symbols.Tilde
            or Symbols.ExclamationMark
            or Symbols.At
            or Symbols.Num
            or Symbols.Percent
            or Symbols.Caret
            or Symbols.Ampersand
            or Symbols.Pipe
            or Symbols.Backtick
            or Symbols.QuestionMark;

    public override Statement? ParseStatement(Parser parser)
    {
        return parser.ParseKeyword(Keyword.COMMENT) ? ParseComment(parser) : null;
    }

    public static Statement.Comment ParseComment(Parser parser)
    {
        var ifExists = parser.ParseIfExists();

        parser.ExpectKeyword(Keyword.ON);
        var token = parser.NextToken();
        CommentObject objectType;
        ObjectName name;

        switch (token)
        {
            case Word { Keyword: Keyword.COLUMN }:
                objectType = CommentObject.Column;
                name = parser.ParseObjectName();
                break;

            case Word { Keyword: Keyword.TABLE }:
                objectType = CommentObject.Table;
                name = parser.ParseObjectName();
                break;

            default:
                throw Parser.Expected("comment object_type", token);
        }

        parser.ExpectKeyword(Keyword.IS);

        var comment = parser.ParseKeyword(Keyword.NULL) ? null : parser.ParseLiteralString();

        return new Statement.Comment(name, objectType, comment, ifExists);
    }

    public override bool SupportsGroupByExpression => true;
    public override bool SupportsUnicodeStringLiteral => true;
}