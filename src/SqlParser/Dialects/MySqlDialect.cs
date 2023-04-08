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
        return character.IsLetter() ||
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
}