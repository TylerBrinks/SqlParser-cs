namespace SqlParser;

internal static class Extensions
{
    /// <summary>
    /// Converts an enumerable object into a sequence of type T
    /// </summary>
    /// <typeparam name="T">Type of items in the sequence</typeparam>
    /// <param name="source">Initial sequence list items</param>
    /// <returns>Sequence of type T</returns>
    public static Sequence<T> ToSequence<T>(this IEnumerable<T> source)
    {
        return new Sequence<T>(source);
    }
    /// <summary>
    /// Checks if an enumerable object is not null and has at
    /// least one element.
    /// </summary>
    /// <typeparam name="T">Type of item in the enumerable list</typeparam>
    /// <param name="enumerable">Enumerable instance to check</param>
    /// <returns>True if not null and items exist; otherwise false</returns>
    public static bool SafeAny<T>(this IEnumerable<T>? enumerable)
    {
        return enumerable != null && enumerable.Any();
    }

    /// <summary>
    /// Fields reserved for data/time operations
    /// </summary>
    public static readonly Keyword[] DateTimeFields = {
        Keyword.YEAR,
        Keyword.MONTH,
        Keyword.WEEK,
        Keyword.DAY,
        Keyword.HOUR,
        Keyword.MINUTE,
        Keyword.SECOND,
        Keyword.CENTURY,
        Keyword.DECADE,
        Keyword.DOW,
        Keyword.DOY,
        Keyword.EPOCH,
        Keyword.ISODOW,
        Keyword.ISOYEAR,
        Keyword.JULIAN,
        Keyword.MICROSECOND,
        Keyword.MICROSECONDS,
        Keyword.MILLENIUM,
        Keyword.MILLENNIUM,
        Keyword.MILLISECOND,
        Keyword.MILLISECONDS,
        Keyword.NANOSECOND,
        Keyword.NANOSECONDS,
        Keyword.QUARTER,
        Keyword.TIMEZONE,
        Keyword.TIMEZONE_HOUR,
        Keyword.TIMEZONE_MINUTE
    };

    /// <summary>
    /// Checks if a character is a digit or letter
    /// </summary>
    /// <param name="c">Character to evaluate</param>
    /// <returns>True if alphanumeric</returns>
    public static bool IsAlphaNumeric(this char c)
    {
        return c.IsDigit() || c.IsLetter();
    }
    /// <summary>
    /// Checks if a character is an ASCII digit (0-9)
    /// </summary>
    /// <param name="c">Character to evaluate</param>
    /// <returns>True if a digit</returns>
    public static bool IsDigit(this char c)
    {
        return c is >= Symbols.Zero and <= Symbols.Nine;
    }
    /// <summary>
    /// Checks if a character is an ASCII letter (a-z, A-Z)
    /// </summary>
    /// <param name="c">Character to evaluate</param>
    /// <returns>True if a letter</returns>
    public static bool IsLetter(this char c)
    {
        return IsUppercaseAscii(c) || IsLowercaseAscii(c);
    }

    public static bool IsAlphabetic(this char c)
    {
        return c.IsLetter() || char.IsLetter(c);
    }
    /// <summary>
    /// Checks if a character is a lower case ASCII letter (a-z)
    /// </summary>
    /// <param name="c">Character to evaluate</param>
    /// <returns>True if a lower case letter</returns>
    public static bool IsLowercaseAscii(this char c)
    {
        return c is >= Symbols.LowerA and <= Symbols.LowerZ;
    }
    /// <summary>
    /// Checks if a character is a upper case ASCII letter (A-Z)
    /// </summary>
    /// <param name="c">Character to evaluate</param>
    /// <returns>True if an upper case letter</returns>
    public static bool IsUppercaseAscii(this char c)
    {
        return c is >= Symbols.CapitalA and <= Symbols.CapitalZ;
    }
    /// <summary>
    /// Checks if a character is a HEX character (a-f, A-F, 0-9_
    /// </summary>
    /// <param name="c"></param>
    /// <returns>True if a hex character</returns>
    public static bool IsHex(this char c)
    {
        return c.IsDigit() || c is >= Symbols.CapitalA and <= Symbols.CapitalF or >= Symbols.LowerA and <= Symbols.LowerF;
    }
    /// <summary>
    /// Escapes quoted strings
    /// </summary>
    /// <param name="value">Value to escape</param>
    /// <param name="quote">Quote character to escape</param>
    /// <returns>Escapee string</returns>
    public static string? EscapeQuotedString(this string? value, char quote)
    {
        if (value == null)
        {
            return value;
        }

        var builder = StringBuilderPool.Get();

        foreach (var character in value)
        {
           builder.Append(character == quote ? $"{quote}{quote}" : $"{character}");
        }

        return StringBuilderPool.Return(builder);
    }
    /// <summary>
    /// Escapes a string with single quotes
    /// </summary>
    /// <param name="value">Value to escape</param>
    /// <returns>Escaped string</returns>
    public static string? EscapeSingleQuoteString(this string? value)
    {
        return EscapeQuotedString(value, Symbols.SingleQuote);
    }
    /// <summary>
    /// Escapes a string by replacing requiring escape substitution
    ///
    /// ' becomes \'
    /// \ becomes \\
    /// [new line] becomes \n
    /// [tab] becomes \t
    /// [return] becomes \r
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static string? EscapeEscapedString(this string? value)
    {
        if (value == null)
        {
            return value;
        }

        var builder = StringBuilderPool.Get();

        foreach (var character in value)
        {
            var escaped = character switch
            {
                Symbols.SingleQuote => @"\'",
                Symbols.Backslash => @"\\",
                Symbols.NewLine => @"\n",
                Symbols.Tab => @"\t",
                Symbols.CarriageReturn => @"\r",
                _ => character.ToString()
            };
            builder.Append(escaped);
        }

        return StringBuilderPool.Return(builder);
    }
}