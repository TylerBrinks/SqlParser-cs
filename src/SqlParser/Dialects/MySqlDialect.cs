using SqlParser.Ast;
using static SqlParser.Ast.Statement;

namespace SqlParser.Dialects;


/// <summary>
/// MySQL grammar
///
/// <see href="https://dev.mysql.com/doc/refman/8.0/en/sql-statements.html"/>
/// </summary>
public class MySqlDialect : Dialect
{
    public override bool IsIdentifierStart(char character)
    {
        return char.IsLetter(character) ||
               character is >= (char)0x0080 and < Symbols.EndOfFile 
               or Symbols.Underscore 
               or Symbols.Dollar 
               or Symbols.At;
    }

    public override bool IsIdentifierPart(char character)
    {
        return IsIdentifierStart(character) || character.IsDigit();
    }

    public override bool IsDelimitedIdentifierStart(char character)
    {
        return character == Symbols.Backtick;
    }

    public override Expression? ParseInfix(Parser parser, Expression expr, int precedence)
    {
        if (parser.ParseKeyword(Keyword.DIV))
        {
            return new Expression.BinaryOp(expr, BinaryOperator.MyIntegerDivide, parser.ParseExpr());
        }

        return base.ParseInfix(parser, expr, precedence);
    }

    public override Statement? ParseStatement(Parser parser)
    {
        if (parser.ParseKeywordSequence(Keyword.LOCK, Keyword.TABLES))
        {
            return new LockTables(parser.ParseCommaSeparated(parser.ParseLockTable));
        }
        
        return parser.ParseKeywordSequence(Keyword.UNLOCK, Keyword.TABLES) ? new UnlockTables() : null;
    }

    public override char? IdentifierQuoteStyle(string identifier) => Symbols.Backtick;

    public override bool SupportsStringLiteralBackslashEscape => true;
    public override bool SupportsNumericPrefix => true;
    public override bool RequireIntervalQualifier => true;
}