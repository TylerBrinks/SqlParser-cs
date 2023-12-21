namespace SqlParser;

/// <summary>
/// Tracks parser depth to prevent deep recursion
/// </summary>
public sealed class DepthGuard(uint remainingDepth)
{
    public DepthGuard? Parent { get; }

    public DepthGuard(uint remainingDepth, DepthGuard parent) : this(remainingDepth)
    {
        Parent = parent;
    }

    public DepthScope Decrement()
    {
        var oldValue = remainingDepth - 1;

        if (oldValue == 0)
        {
            throw new ParserException("Recursion limit exceeded.");
        }

        remainingDepth--;

        return new DepthScope(this);
    }

    public void Increment()
    {
        remainingDepth++;
    }
}