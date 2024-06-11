namespace SqlParser;

public enum ParserState
{
    // The default state of the parser.
    Normal,
    /// The state when parsing a CONNECT BY expression. This allows parsing
    /// PRIOR expressions while still allowing prior as an identifier name
    /// in other contexts.
    ConnectBy
}