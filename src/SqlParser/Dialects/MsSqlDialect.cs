using SqlParser.Ast;
using SqlParser.Tokens;

namespace SqlParser.Dialects;

/// <summary>
/// MS SQL dialect
///
/// <see href="https://learn.microsoft.com/en-us/sql/t-sql/language-elements/language-elements-transact-sql?view=sql-server-ver16"/>
/// </summary>
public class MsSqlDialect : Dialect
{
    public override Statement? ParseStatement(Parser parser)
    {
        return parser.ParseKeyword(Keyword.IF) ? ParseIfStatement(parser) : null;
    }

    public override bool IsDelimitedIdentifierStart(char character)
    {
        return character is Symbols.DoubleQuote or Symbols.SquareBracketOpen;
    }

    public override bool IsIdentifierStart(char character)
    {
        // See https://docs.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers?view=sql-server-2017#rules-for-regular-identifiers
        // We don't support non-latin "letters" currently.

        return character.IsLetter() ||
               character is Symbols.Underscore
                   or Symbols.Num
                   or Symbols.At;
    }

    public override bool IsIdentifierPart(char character)
    {
        return character.IsAlphaNumeric() ||
               character is Symbols.At
                   or Symbols.Dollar
                   or Symbols.Num
                   or Symbols.Underscore;
    }

    public override bool SupportsSubstringFromForExpression => false;
    public override bool ConvertTypeBeforeValue => true;
    public override bool SupportsConnectBy => true;
    public override bool SupportsEqualAliasAssignment => true;
    public override bool SupportsTryConvert => true;

    private static Statement ParseIfStatement(Parser parser)
    {
        var condition = parser.ParseExpr();

        if (parser.ParseKeyword(Keyword.THEN))
        {
            var thenBody = parser.ParseStatementBlock();
            var elseIfs = new Sequence<IfStatementElseIf>();

            while (parser.ParseKeyword(Keyword.ELSEIF))
            {
                var elseIfCondition = parser.ParseExpr();
                parser.ExpectKeyword(Keyword.THEN);
                var elseIfBody = parser.ParseStatementBlock();
                elseIfs.Add(new IfStatementElseIf(elseIfCondition, elseIfBody));
            }

            Sequence<Statement>? elseBlock = null;
            if (parser.ParseKeyword(Keyword.ELSE))
            {
                elseBlock = parser.ParseStatementBlock();
            }

            parser.ExpectKeywords(Keyword.END, Keyword.IF);

            return new Statement.If(new IfStatement(condition, thenBody)
            {
                ElseIfs = elseIfs.Any() ? elseIfs : null,
                ElseBlock = elseBlock
            });
        }

        var thenBlock = ParseIfBranchBody(parser);
        parser.ConsumeToken<SemiColon>();

        Sequence<Statement>? elseBlockStatement = null;
        if (parser.ParseKeyword(Keyword.ELSE))
        {
            elseBlockStatement = ParseIfBranchBody(parser);
            parser.ConsumeToken<SemiColon>();
        }

        return new Statement.If(new IfStatement(condition, thenBlock)
        {
            ElseBlock = elseBlockStatement,
            Syntax = IfStatementSyntax.MsSql
        });
    }

    private static Sequence<Statement> ParseIfBranchBody(Parser parser)
    {
        if (!parser.ParseKeyword(Keyword.BEGIN))
        {
            return [parser.ParseStatement()];
        }

        var statements = parser.ParseStatementBlock();
        parser.ExpectKeyword(Keyword.END);
        return statements;
    }
}
