namespace SqlParser.Tokens;

public abstract record NumStringQuoteChars
{
    public record One : NumStringQuoteChars;

    public record Many(ushort Count) : NumStringQuoteChars;

    public record Unused : NumStringQuoteChars;
}