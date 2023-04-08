using System.Text;
using Microsoft.Extensions.ObjectPool;

namespace SqlParser;

internal static class StringBuilderPool
{
    private static readonly ObjectPool<StringBuilder> Pool = new DefaultObjectPoolProvider().CreateStringBuilderPool();

    /// <summary>
    /// Leases a StringBuilder from the object pool
    /// </summary>
    /// <returns>StringBuilder instance</returns>
    internal static StringBuilder Get()
    {
        return Pool.Get();
    }

    /// <summary>
    /// Helper wrapper to return the string builder's value and return the
    /// builder to the pool at the same time.
    /// </summary>
    /// <param name="builder">Builder to add back to the pool</param>
    /// <returns>StringBuilder's ToString output</returns>
    internal static string Return(StringBuilder builder)
    {
        try
        {
            return builder.ToString();
        }
        finally
        {
            Pool.Return(builder);
        }
    }
}
