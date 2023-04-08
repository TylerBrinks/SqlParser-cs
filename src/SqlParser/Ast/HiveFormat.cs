namespace SqlParser.Ast;

/// <summary>
/// Hive-specific format
/// </summary>
public record HiveFormat: IElement
{
    public HiveRowFormat? RowFormat { get; internal set; }
    public HiveIOFormat? Storage { get; internal set; }
    public string? Location { get; internal set; }
}
/// <summary>
/// Hive row format
/// </summary>
public abstract record HiveRowFormat
{
    /// <summary>
    /// Hive Serde row format
    /// </summary>
    /// <param name="Class">String class name</param>
    public record Serde(string Class) : HiveRowFormat;
    /// <summary>
    /// Hive delimited row format
    /// </summary>
    public record Delimited : HiveRowFormat;
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
    /// Hive clustered distribution
    /// </summary>
    public record Clustered : HiveDistributionStyle
    {
        public Sequence<Ident>? Columns { get; init; }
        public Sequence<ColumnDef>? SortedBy { get; init; }
        public int NumBuckets { get; init; }
    }
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
public abstract record HiveIOFormat
{
    /// <summary>
    /// Hive IOF format
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public record IOF(Expression InputFormat, Expression OutputFormat) : HiveIOFormat, IElement;
    /// <summary>
    /// Hive File IO format
    /// </summary>
    public record FileFormat : HiveIOFormat
    {
        public Ast.FileFormat Format { get; init; }
    }
}
