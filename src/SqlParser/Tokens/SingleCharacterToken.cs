namespace SqlParser.Tokens;

public class SingleCharacterToken(char character) : Token
{
    public char Character { get; } = character;

    public override string ToString()
    {
        return Character.ToString();
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as SingleCharacterToken);
    }

    protected virtual bool Equals(SingleCharacterToken? other)
    {
        return other != null && Character == other.Character;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Character);
    }
}