using SqlParser.Ast;

namespace SqlParser.Tokens;

public enum WhitespaceKind
{
    Space,
    Tab,
    NewLine,
    InlineComment,
    MultilineComment
}

/// <summary>
/// A keyword (like SELECT) or an optionally quoted SQL identifier
/// </summary>
public class Word : StringToken
{
    public Word(string value) : this(value, null)
    {
    }

    public Word(string value, char? quoteCharacter = null) : base(value)
    {
        QuoteStyle = quoteCharacter;

        if (QuoteStyle is not null)
        {
            return;
        }

        var index = Array.BinarySearch(Keywords.All, value.ToUpperInvariant());

        if (index > -1)
        {
            Keyword = (Keyword)index;
        }
    }

    public char? QuoteStyle { get; init; }

    public Keyword Keyword { get; init; } = Keyword.undefined;

    public Ident ToIdent()
    {
        return new Ident(Value, QuoteStyle);
    }

    public override string ToString()
    { 
        var quote = $"{(QuoteStyle.HasValue ? GetEndQuote(QuoteStyle.Value) : null)}";

        return $"{quote}{Value}{quote}";
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as Word);
    }

    protected bool Equals(Word? other)
    {
        return other != null && Value == other.Value && QuoteStyle == other.QuoteStyle && Keyword == other.Keyword;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Value, QuoteStyle);
    }

    public static char GetEndQuote(char startQuote)
    {
        return startQuote switch
        {
            Symbols.DoubleQuote => Symbols.DoubleQuote, // ANSI and most dialects
            Symbols.SquareBracketOpen => Symbols.SquareBracketClose, // MS SQL
            Symbols.Backtick => Symbols.Backtick, // MySQL
            _ => throw new ArgumentException("unexpected quoting style!"),
        };
    }
}