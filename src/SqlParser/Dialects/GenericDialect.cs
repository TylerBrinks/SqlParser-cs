namespace SqlParser.Dialects;

/// <summary>
/// Generic SQL dialect
/// </summary>
public class GenericDialect : Dialect
{
    public override bool IsIdentifierStart(char character)
    {
        return char.IsLetter(character) ||
               character is Symbols.Underscore
                   or Symbols.Num
                   or Symbols.At;
    }

    public override bool IsIdentifierPart(char character)
    {
        return char.IsLetter(character) ||
               char.IsDigit(character) ||
               character is Symbols.At 
                   or Symbols.Dollar 
                   or Symbols.Num 
                   or Symbols.Underscore;
    }
}