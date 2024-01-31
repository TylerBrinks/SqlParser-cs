namespace SqlParser.Tokens;

/// <summary>
/// Ampersand &
/// </summary>
public class Ampersand() : SingleCharacterToken(Symbols.Ampersand)
{
    public override bool Equals(object? obj)
    {
        return base.Equals(obj) && Equals(obj as Ampersand);
    }

    protected bool Equals(Ampersand? other)
    {
        return other != null && Character == other.Character;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Symbols.Ampersand);
    }
}