namespace SqlParser.Ast;

/// <summary>
/// XMLTABLE expression
/// </summary>
public record XmlTableExpression(
    Expression RowExpression,
    Expression PassingExpression,
    Sequence<XmlTableColumn> Columns) : IWriteSql, IElement
{
    public Sequence<XmlNamespace>? Namespaces { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("XMLTABLE(");

        if (Namespaces.SafeAny())
        {
            writer.WriteSql($"XMLNAMESPACES({Namespaces.ToSqlDelimited()}), ");
        }

        writer.WriteSql($"{RowExpression} PASSING {PassingExpression} COLUMNS {Columns.ToSqlDelimited()})");
    }
}

/// <summary>
/// XMLTABLE column definition
/// </summary>
public record XmlTableColumn(
    Ident Name,
    DataType DataType) : IWriteSql, IElement
{
    public Expression? Path { get; init; }
    public Expression? Default { get; init; }
    public bool NotNull { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name} {DataType}");

        if (Path != null)
        {
            writer.WriteSql($" PATH {Path}");
        }

        if (Default != null)
        {
            writer.WriteSql($" DEFAULT {Default}");
        }

        if (NotNull)
        {
            writer.Write(" NOT NULL");
        }
    }
}

/// <summary>
/// XML namespace declaration
/// </summary>
public record XmlNamespace(Value Uri, Ident? Prefix = null) : IWriteSql, IElement
{
    public bool IsDefault { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Uri}");

        if (IsDefault)
        {
            writer.Write(" AS DEFAULT");
        }
        else if (Prefix != null)
        {
            writer.WriteSql($" AS {Prefix}");
        }
    }
}
