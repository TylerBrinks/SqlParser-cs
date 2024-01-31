// ReSharper disable InconsistentNaming
namespace SqlParser.Tokens;

/// <summary>
/// An end-of-file marker, not a real token
/// </summary>
public class EOF() : StringToken("EOF")
{
    public override bool Equals(object? obj)
    {
        return obj is EOF;
    }

    public override int GetHashCode()
    {
        return 42; // Constant value.  All EoF are considered the same.
    }
}