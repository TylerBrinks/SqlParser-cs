namespace SqlParser.Ast;

/// <summary>
/// Hive-specific format
/// </summary>
public record HiveFormat : IElement
{
    public HiveRowFormat? RowFormat { get; internal set; }
    public Sequence<SqlOption>? SerdeProperties { get; internal set; }
    public HiveIOFormat? Storage { get; internal set; }
    public string? Location { get; internal set; }
}
/// <summary>
/// Hive row format
/// </summary>
public abstract record HiveRowFormat : IElement
{
    /// <summary>
    /// Hive Serde row format
    /// </summary>
    /// <param name="Class">String class name</param>
    public record Serde(string Class) : HiveRowFormat;
    /// <summary>
    /// Hive delimited row format
    /// </summary>
    public record Delimited(Sequence<HiveRowDelimiter>? Delimiters) : HiveRowFormat;
}

public record HiveRowDelimiter(HiveDelimiter Delimiter, Ident Character) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Delimiter} {Character}");
    }
}
/// <summary>
/// Hive distribution style
/// </summary>
public abstract record HiveDistributionStyle : IElement
{
    /// <summary>
    /// Hive partitioned distribution
    /// </summary>
    /// <param name="Columns"></param>
    public record Partitioned(Sequence<ColumnDef> Columns) : HiveDistributionStyle;
    /// <summary>
    /// Hive skewed distribution
    /// </summary>
    public record Skewed(Sequence<ColumnDef> Columns, Sequence<ColumnDef> On) : HiveDistributionStyle
    {
        public bool StoredAsDirectories { get; init; }
    }
    /// <summary>
    /// Hive no distribution style
    /// </summary>
    public record None : HiveDistributionStyle;
}

/// <summary>
/// Hive IO format
/// </summary>
// ReSharper disable once InconsistentNaming
public abstract record HiveIOFormat : IElement
{
    /// <summary>
    /// Hive IOF format
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public record IOF(Expression InputFormat, Expression OutputFormat) : HiveIOFormat;
    /// <summary>
    /// Hive File IO format
    /// </summary>
    public record FileFormat : HiveIOFormat
    {
        public Ast.FileFormat Format { get; init; }
    }
}

public record HiveSetLocation(bool HasSet, Ident Location) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        if (HasSet)
        {
            writer.Write("SET ");
        }

        writer.WriteSql($"LOCATION {Location}");
    }
}