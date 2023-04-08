namespace SqlParser.Tokens;

/// <summary>
/// Whitespace (space, tab, etc)
/// </summary>
public class Whitespace : Token
{
    public Whitespace(WhitespaceKind kind) : this(kind, null)
    {
    }

    public Whitespace(WhitespaceKind kind, string? value)
    {
        WhitespaceKind = kind;
        Value = value;
    }

    public WhitespaceKind WhitespaceKind { get; init; }
    public string? Prefix { get; init; }
    public string? Value { get; init; }

    public override bool Equals(object? obj)
    {
        return Equals(obj as Whitespace);
    }

    protected bool Equals(Whitespace? other)
    {
        return other != null && WhitespaceKind == other.WhitespaceKind && Prefix == other.Prefix && Value == other.Value;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(WhitespaceKind, Prefix, Value);
    }

    public override string ToString()
    {
        return WhitespaceKind switch
        {
            WhitespaceKind.NewLine => Symbols.NewLine.ToString(),
            WhitespaceKind.Space => Symbols.Space.ToString(),
            WhitespaceKind.Tab => Symbols.Tab.ToString(),
            WhitespaceKind.InlineComment => $"{Prefix}{Value}",
            WhitespaceKind.MultilineComment => $"/*{Value}*/",
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}