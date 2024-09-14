using System.ComponentModel.DataAnnotations;

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
    public static readonly Keyword[] DateTimeFields = [
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
    ];

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
    /// Checks if the character is an EOF character.
    /// </summary>
    /// <param name="c">Character to evaluate</param>
    /// <returns>True if end of file (char.MaxValue); otherwise false</returns>
    public static bool IsEoF(this char c)
    {
        return c == Symbols.EndOfFile;
    }
    /// <summary>
    /// Escapes quoted strings
    /// </summary>
    /// <param name="value">Value to escape</param>
    /// <param name="quote">Quote character to escape</param>
    /// <returns>Escapee string</returns>
    public static string? EscapeQuotedString(this string? value, char quote)
    {
        // EscapeQuotedString doesn't know which mode of escape was
        // chosen by the user. So this code must to correctly display
        // strings without knowing if the strings are already escaped
        // or not.
        //
        // If the quote symbol in the string is repeated twice, OR, if
        // the quote symbol is after backslash, display all the chars
        // without any escape. However, if the quote symbol is used
        // just between usual chars, `fmt()` should display it twice.
        //
        // The following table has examples
        //
        // | original query | mode      | AST Node                                           | serialized   |
        // | -------------  | --------- | -------------------------------------------------- | ------------ |
        // | `"A""B""A"`    | no-escape | `DoubleQuotedString(String::from("A\"\"B\"\"A"))`  | `"A""B""A"`  |
        // | `"A""B""A"`    | default   | `DoubleQuotedString(String::from("A\"B\"A"))`      | `"A""B""A"`  |
        // | `"A\"B\"A"`    | no-escape | `DoubleQuotedString(String::from("A\\\"B\\\"A"))`  | `"A\"B\"A"`  |
        // | `"A\"B\"A"`    | default   | `DoubleQuotedString(String::from("A\"B\"A"))`      | `"A""B""A"`  |

        if (string.IsNullOrEmpty(value))
        {
            return null;
        }

        var previousChar = char.MinValue;
        var state = new State(value);
        char current;
        List<char> word = [];

        while ((current = state.Peek()) != char.MaxValue)
        {
            if (current == quote)
            {
                if (previousChar == Symbols.Backslash)
                {
                    word.Add(current);
                    state.Next();
                    continue;
                }

                state.Next();
                if (state.Peek() == quote)
                {
                    word.Add(current);
                    word.Add(current);
                    state.Next();
                }
                else
                {
                    word.Add(current);
                    word.Add(current);
                }
            }
            else
            {
                word.Add(current);
                state.Next();
            }

            previousChar = current;
        }

        return new string(word.ToArray());
    }
    /// <summary>
    /// Escapes a string with single quotes
    /// </summary>
    /// <param name="value">Value to escape</param>
    /// <returns>Escaped string</returns>
    public static string? EscapeSingleQuoteString(this string? value) => EscapeQuotedString(value, Symbols.SingleQuote);
    /// <summary>
    /// Escapes a string with double quotes
    /// </summary>
    /// <param name="value">Value to escape</param>
    /// <returns>Escaped string</returns>
    public static string? EscapeDoubleQuoteString(this string? value) => EscapeQuotedString(value, Symbols.DoubleQuote);

    public static string? EscapeUnicodeString(this string? value)
    {
        if (value == null)
        {
            return value;
        }

        var builder = StringBuilderPool.Get();

        foreach (var ch in value)
        {

            switch (ch)
            {
                case Symbols.SingleQuote:
                    builder.Append("''");
                    break;
                case Symbols.Backslash:
                    builder.Append(@"\\");
                    break;
                case Symbols.NewLine:
                    builder.Append(@"\n");
                    break;

                default:
                {
                    if (char.IsAscii(ch))
                    {
                        builder.Append(ch);
                    }
                    else
                    {
                        uint codepoint = ch;
                        // if the character fits in 32 bits, we can use the \XXXX format
                        // otherwise, we need to use the \+XXXXXX format
                        if (codepoint < 0xFFFF)
                        {
                            builder.Append($"\\{ch:XXXX}");
                        }
                        else
                        {
                            builder.Append($"\\+{ch:XXXXXX}");
                        }
                    }

                    break;
                }
            }
        }

        return StringBuilderPool.Return(builder);
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

        foreach (var ch in value)
        {
            switch (ch)
            {
                case Symbols.SingleQuote:
                    builder.Append(@"\'");
                    break;
                case Symbols.Backslash:
                    builder.Append(@"\\");
                    break;
                case Symbols.NewLine:
                    builder.Append(@"\n");
                    break;
                case Symbols.Tab:
                    builder.Append(@"\t");
                    break;
                case Symbols.CarriageReturn:
                    builder.Append(@"\r");
                    break;
                default:
                    builder.Append(ch);
                    break;
            }
        }

        return StringBuilderPool.Return(builder);
    }
}