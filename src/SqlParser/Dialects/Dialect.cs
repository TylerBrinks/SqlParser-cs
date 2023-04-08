using SqlParser.Ast;

namespace SqlParser.Dialects;

/// <summary>
/// Base dialect definition
/// </summary>
public abstract class Dialect
{
    /// <summary>
    /// Determine if a character is the start of an identifier
    /// </summary>
    /// <param name="character">Character to test</param>
    /// <returns>True if the start of an ident; otherwise false.</returns>
    public abstract bool IsIdentifierStart(char character);
    /// <summary>
    /// Determine if a character is the part of an identifier
    /// </summary>
    /// <param name="character">Character to test</param>
    /// <returns>True if part of an ident; otherwise false.</returns>
    public abstract bool IsIdentifierPart(char character);
    /// <summary>
    /// Determine if a character starts a quoted identifier. The default
    /// implementation, accepting "double quoted" ids is both ANSI-compliant
    /// and appropriate for most dialects (with the notable exception of
    /// MySQL, MS SQL, and sqlite). You can accept one of characters listed
    /// in `Word::matching_end_quote` here
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if ident start; otherwise false</returns>
    public virtual bool IsDelimitedIdentifierStart(char character)
    {
        return character == Symbols.DoubleQuote;
    }
    /// <summary>
    /// Determine if quoted characters are proper for identifier
    /// </summary>
    /// <returns>True if proper; otherwise false.</returns>
    public virtual bool IsProperIdentifierInsideQuotes(State state)
    {
        return true;
    }
    /// <summary>
    /// True if the dialect supports filtering during aggregation
    /// </summary>
    /// <returns>True if supported; otherwise false.</returns>
    public virtual bool SupportsFilterDuringAggregation()
    {
        return false;
    }
    /// <summary>
    /// Returns true if the dialect supports `ARRAY_AGG() [WITHIN GROUP (ORDER BY)]` expressions.
    /// Otherwise, the dialect should expect an `ORDER BY` without the `WITHIN GROUP` clause, e.g. [`ANSI`]
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#array-aggregate-function"/>
    /// </summary>
    /// <returns>True if supported; otherwise false</returns>
    public virtual bool SupportsWithinAfterArrayAggregation()
    {
        return false;
    }
    /// <summary>
    /// Allow dialect implementations to override statement parsing
    /// </summary>
    /// <param name="parser">Parser instance</param>
    /// <returns>Parsed Expression</returns>
    public virtual Statement? ParseStatement(Parser parser)
    {
        return null;
    }
    /// <summary>
    /// Allow dialect implementations to override prefix parsing
    /// </summary>
    /// <param name="parser">Parser instance</param>
    /// <returns>Parsed Expression</returns>
    public virtual Expression? ParsePrefix(Parser parser)
    {
        return null;
    }
    /// <summary>
    /// Dialect-specific infix parser override
    /// </summary>
    /// <param name="parser">Parser instance</param>
    /// <param name="Expression">Expression</param>
    /// <param name="precedence">Token precedence</param>
    /// <returns>Parsed Expression</returns>
    public virtual Expression? ParseInfix(Parser parser, Expression expr, int precedence)
    {
        return null;

    }
    /// <summary>
    /// Allow dialect implementations to override parser token precedence
    /// </summary>
    /// <param name="parser">Parser instance</param>
    /// <returns>Token Precedence</returns>
    public virtual short? GetNextPrecedence(Parser parser)
    {
        return null;
    }
}