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
        //return character.IsLetter() || character == Symbols.Underscore;
        return char.IsLetter(character) || character == Symbols.Underscore;
    }

    public override bool IsIdentifierPart(char character)
    {
        //return character.IsAlphaNumeric() || character is Symbols.Dollar or Symbols.Underscore;
        return char.IsLetterOrDigit(character) || character is Symbols.Dollar or Symbols.Underscore;
    }

    public override bool SupportsFilterDuringAggregation()
    {
        return true;
    }

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
            case Word {Keyword: Keyword.COLUMN}:
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
}