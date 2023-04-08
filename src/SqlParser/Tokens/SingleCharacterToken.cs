namespace SqlParser.Tokens;

public class SingleCharacterToken : Token
{
    public SingleCharacterToken(char character)
    {
        Character = character;
    }

    public char Character { get; }

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