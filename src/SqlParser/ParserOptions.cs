namespace SqlParser;

/// <summary>
/// Configures parser behavior
/// </summary>
public class ParserOptions
{
    /// <summary>
    /// Set to true to allow trailing commas in
    /// projections and comma-delimited lists
    /// </summary>
    public bool TrailingCommas { get; set; }
    /// <summary>
    /// Sets the recursion depth beyond which the parser will
    /// throw an exception.  The default is 50.
    /// </summary>
    public uint RecursionLimit { get; set; } = 50;
    /// <summary>
    /// Controls how literal values are unescaped.
    /// </summary>
    public bool Unescape { get; set; } = true;
}