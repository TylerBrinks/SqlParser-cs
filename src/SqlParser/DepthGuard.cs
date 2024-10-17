namespace SqlParser;

/// <summary>
/// Tracks parser depth to prevent deep recursion
/// <param name="remainingDepth">Creates a root depth guard with a specified remaining depth</param>
/// </summary>
public sealed class DepthGuard(uint remainingDepth)
{
    private uint _remainingDepth = remainingDepth;
    public DepthGuard? Parent { get; }

    public DepthGuard(uint remainingDepth, DepthGuard parent) : this(remainingDepth)
    {
        Parent = parent;
    }

    public DepthScope Decrement()
    {
        var oldValue = _remainingDepth - 1;

        if (oldValue == 0)
        {
            throw new ParserException("Recursion limit exceeded.");
        }

        _remainingDepth--;

        return new DepthScope(this);
    }

    public void Increment()
    {
        _remainingDepth++;
    }
}