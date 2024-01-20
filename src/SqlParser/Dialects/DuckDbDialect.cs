using SqlParser.Dialects;
using SqlParser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlParser.Dialects;

/// <summary>
/// DuckDb SQL Dialect
/// </summary>
public class DuckDbDialect : Dialect
{
    /// <summary>
    /// Checks if a given character is an ASCII letter 
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if an identifier; otherwise false</returns>
    public override bool IsIdentifierStart(char character)
    {
        return character.IsAlphabetic() || character == Symbols.Underscore;
    }
    /// <summary>
    /// Checks if a given character is an ASCII letter, number, underscore, or minus
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if an identifier part; otherwise false</returns>
    public override bool IsIdentifierPart(char character)
    {
        return IsIdentifierStart(character) || character.IsDigit() || character == Symbols.Dollar;
    }

    /// <summary>
    /// Suports filter during aggregation is always true
    /// </summary>
    /// <returns>True</returns>
    public override bool SupportsFilterDuringAggregation => true;
}