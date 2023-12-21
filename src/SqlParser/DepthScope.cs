namespace SqlParser;

/// <summary>
/// Produced by the depth guard class, this facade handles
/// scope for depth trakcing and increments the parent
/// guard's remaining depth value when the instance goes
/// out of scope and is disposed.
/// </summary>
/// <param name="guard">Parent guard object to increment upon completing of a scoped operation</param>
public sealed class DepthScope(DepthGuard guard) : IDisposable
{
    public void Dispose() => Dispose(true);

    ~DepthScope() => Dispose(false);

    private void Dispose(bool disposing)
    {
        guard.Increment();

        if (disposing)
        {
            GC.SuppressFinalize(this);
        }
    }
}