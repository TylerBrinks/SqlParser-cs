using SqlParser.Ast;
using SqlParser.Dialects;

namespace SqlParser;

/// <summary>
/// SQL Parser wrapper class
/// </summary>
public class SqlQueryParser
{
    private readonly Parser _parser = new();

    /// <summary>
    /// Parses a given SQL string into an Abstract Syntax Tree with a generic SQL dialect
    /// </summary>
    /// <param name="sql">SQL string to parse</param>
    /// <param name="options">Parsing options</param>
    /// <returns></returns>
    /// <exception cref="TokenizeException">Thrown when an unexpected token in encountered while parsing the input string</exception>
    /// <exception cref="ParserException">Thrown when the sequence of tokens does not match the dialect's expected grammar</exception>
    /// <returns>Sequence of SQL Statement syntax tree instances</returns>
    public Sequence<Statement> Parse(ReadOnlySpan<char> sql, ParserOptions? options = null)
        => Parse(sql, new GenericDialect(), options);
    /// <summary>
    /// Parses a given SQL string into an Abstract Syntax Tree using a given SQL dialect
    /// </summary>
    /// <param name="sql">SQL string to parse</param>
    /// <param name="dialect">SQL dialect instance</param>
    /// <param name="options">Parsing options</param>
    /// <returns>Sequence of SQL Statement syntax tree instances</returns>
    public Sequence<Statement> Parse(ReadOnlySpan<char> sql, Dialect dialect, ParserOptions? options = null)
        => _parser.ParseSql(sql, dialect, options);
}
