namespace SqlParser.Ast;

/// <summary>
/// Object name containing one more named identifiers
/// </summary>
public record ObjectName : IWriteSql, IElement
{
    /// <summary>
    /// Single identifier object name
    /// </summary>
    /// <param name="name">Name identifier</param>
    public ObjectName(Ident name)
    {
        Values.Add(name);
    }
    /// <summary>
    /// Multiple identifier object name
    /// </summary>
    /// <param name="names">Name identifiers</param>
    public ObjectName(IEnumerable<Ident> names)
    {
        Values.AddRange(names);
    }

    public Sequence<Ident> Values { get; init; } = new();

    public static implicit operator string(ObjectName name)
    {
        return name.ToString();
    }

    public static implicit operator ObjectName(string name)
    {
        return new ObjectName((Ident)name);
    }

    public override string ToString()
    {
        return string.Join(".", (IEnumerable<Ident>)Values);
    }

    public void ToSql(SqlTextWriter writer)
    {
        // Could simply call ToString, but this keeps all string
        // building on the text writer's StringBuilder
        for (var i = 0; i < Values.Count; i++)
        {
            if (i > 0)
            {
                writer.Write(".");
            }

            Values[i].ToSql(writer);
        }
    }
}