namespace SqlParser.Tokens;

/// <summary>
/// DoubleColon `::` (used for casting in postgresql)
public class DoubleColon : StringToken
{
    public DoubleColon() : base("::")
    {
    }
}