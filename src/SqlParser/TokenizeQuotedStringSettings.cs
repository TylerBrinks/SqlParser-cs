using SqlParser.Tokens;

namespace SqlParser;

public class TokenizeQuotedStringSettings
{
    public char QuoteStyle { get; set; }
    public NumStringQuoteChars NumberQuoteCharacters { get; set; }
    public int NumberOpeningQuotesToConsume { get; set; }
    public bool BackslashEscape { get; set; }
}