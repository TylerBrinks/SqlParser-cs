namespace SqlParser;

/// <summary>
/// Tracks parser depth to prevent deep recursion
/// </summary>
public sealed class DepthGuard
{
    private uint _remainingDepth;

    public DepthGuard(uint remainingDepth)
    {
        _remainingDepth = remainingDepth;
    }

    public void Decrease()
    {
        var oldValue = _remainingDepth;

        if (oldValue == 0)
        {
            throw new ParserException("Recursion limit exceeded.");
        }

        _remainingDepth--;
    }
}