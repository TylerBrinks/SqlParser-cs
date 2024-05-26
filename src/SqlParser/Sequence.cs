using SqlParser.Ast;

namespace SqlParser;

/// <summary>
/// List Implementation supporting sequence-based value equality
/// instead of referential equality
/// </summary>
/// <typeparam name="T">The type of element in the sequence</typeparam>
public class Sequence<T> : List<T>, IWriteSql, IElement
{
    public Sequence()
    {
    }

    public Sequence(IEnumerable<T> collection) : base(collection)
    {
    }

    /// <summary>
    /// Convenience method to forward the ToString output of the child
    /// elements instead of writing the Reflection-based name of the 
    /// genetic list type
    /// </summary>
    /// <returns>String-based display of the sequence elements</returns>
    public override string ToString()
    {
        return $"[{string.Join(", ", this)}]";
    }
    /// <summary>
    /// Checks if two sequences are equal.
    /// </summary>
    /// <param name="obj">Sequence to compare</param>
    /// <returns>True if equal</returns>
    public override bool Equals(object? obj)
    {
        return Equals(obj as Sequence<T>);
    }
    /// <summary>
    /// Checks equality of each item in the sequence
    /// </summary>
    /// <param name="set"></param>
    /// <returns></returns>
    protected bool Equals(Sequence<T>? set)
    {
        return set != null && this.SequenceEqual(set);
    }

    /// <summary>
    /// The parser uses the sequence of elements to determine uniqueness.
    /// This would otherwise cause issues when T is a type where the
    /// underlying hash code is not fixed for similar values (e.g. strings).
    /// However for the purposes of the parser, virtually all
    /// equality checking is limited to records that check value equality.
    /// </summary>
    /// <returns>Generated hash code</returns>
    public override int GetHashCode()
    {
        var hash = new HashCode();

        foreach (var item in this)
        {
            hash.Add(item);
        }

        return hash.ToHashCode();
    }

    public void ToSql(SqlTextWriter writer)
    {
        var enumerable = this.ToList();

        for (var i = 0; i < enumerable.Count; i++)
        {
            if (i > 0)
            {
                writer.Write(", ");
            }
            var item = enumerable[i];
            if (item is IWriteSql sql)
            {
                sql.ToSql(writer);
            }
            else
            {
                writer.Write(item?.ToString());
            }
        }
    }

    public ControlFlow Visit(Visitor visitor)
    {
        if (!typeof(T).IsAssignableTo(typeof(IElement)))
        {
            return ControlFlow.Continue;
        }

        return this.Select(item => (item as IElement)!
            .Visit(visitor))
            .FirstOrDefault(control => control == ControlFlow.Break);
    }

    public static implicit operator T[](Sequence<T> list)
    {
        return list.ToArray();
    }

    public static implicit operator Sequence<T>(T[] array)
    {
        return new Sequence<T>(array);
    }
}
