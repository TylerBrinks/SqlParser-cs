using System.Text;
using SqlParser.Tokens;

namespace SqlParser;

public ref struct Unescaper(ref State state)
{
    private State _state = state;

    public string? Unescape()
    {
        var unescaped = new StringBuilder();
        _state.Next();

        char? current;

        while ((current = _state.Next()) is not null)
        {
            if (current == Symbols.SingleQuote)
            {
                // case: ''''
                if (_state.Peek() is Symbols.SingleQuote)
                {
                    _state.Next();
                    unescaped.Append(Symbols.SingleQuote);
                }

                return unescaped.ToString();
            }

            if (current != Symbols.Backslash)
            {
                unescaped.Append(current);
                continue;
            }

            var next = _state.Next();

            var c = next switch
            {
                'b' => '\u0008',
                'f' => '\u000C',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'u' => UnescapeUnicode16(),
                'U' => UnescapeUnicode32(),
                'x' => UnescapeHex(),
                not null when char.IsDigit(next.Value) => UnescapeOctal(next.Value),
                _ => next
            };

            unescaped.Append(CheckNull(c));
        }

        return null;
    }

    public string? UnescapeUnicode()
    {
        var unescaped = StringBuilderPool.Get();
        _state.Next();

        try
        {
            char? current;
            while ((current = _state.Next()) is not null)
            {
                switch (current)
                {
                    case Symbols.SingleQuote:
                        {
                            if (_state.Peek() is Symbols.SingleQuote)
                            {
                                _state.Next();
                                unescaped.Append(Symbols.SingleQuote);
                            }
                            else
                            {
                                return StringBuilderPool.Return(unescaped);
                            }

                            break;
                        }

                    case Symbols.Backslash:
                    {
                        var next = _state.Peek();
                        switch (next)
                        {
                            case Symbols.Backslash:
                                _state.Next();
                                unescaped.Append(Symbols.Backslash);
                                break;

                            case Symbols.Plus:
                                _state.Next();
                                var builder = StringBuilderPool.Get();
                                for (var i = 0; i < 6; i++)
                                {
                                    builder.Append(_state.Next());
                                }

                                var hexValue = Convert.ToInt32(StringBuilderPool.Return(builder), 16);
                                unescaped.Append(char.ConvertFromUtf32(hexValue));
                                break;
                            default:
                                unescaped.Append(UnescapeUnicode16());
                                break;
                        }

                        break;
                    }

                    default:
                        unescaped.Append(current);
                        break;
                }
            }
        }
        catch
        {
            throw new TokenizeException("Unterminated unicode encoded string literal", _state.CloneLocation());
        }

        return unescaped.ToString();
    }

    private static char? CheckNull(char? character)
    {
        if (character == '\0')
        {
            return null;
        }

        return character;
    }

    private char? UnescapeUnicode16()
    {
        return UnescapeUnicode(4);
    }

    private char? UnescapeUnicode32()
    {
        return UnescapeUnicode(8);
    }

    private char? UnescapeHex()
    {
        var builder = new StringBuilder();

        for (var i = 0; i < 2; i++)
        {
            var nextHexDigit = NextHexDigit();

            if (nextHexDigit == null)
            {
                break;
            }

            builder.Append((char)nextHexDigit);
        }

        if (builder.Length == 0)
        {
            return 'x';
        }

        return ByteToChar(builder.ToString(), 16);
    }

    private char? UnescapeOctal(char character)
    {
        var builder = new StringBuilder();
        builder.Append(character);

        for (var i = 0; i < 2; i++)
        {
            var digest = NextOctalDigest();
            if (digest != null)
            {
                builder.Append(character);
            }
            else
            {
                break;
            }
        }

        return ByteToChar(builder.ToString(), 8);
    }

    private char? UnescapeUnicode(int size)
    {
        var builder = new StringBuilder();

        for (var i = 0; i < size; i++)
        {
            builder.Append(_state.Next());
        }

        try
        {
            var converted = FromStringRadix(builder.ToString(), 16);
            var characterString = char.ConvertFromUtf32(converted);
            return characterString[0];
        }
        catch
        {
            return null;
        }
    }

    private static int FromStringRadix(string value, int radix)
    {
        var numeric = radix == 8 ? Convert.ToUInt16(value, 8) : Convert.ToUInt32(value, 16);
        return (int)numeric;
    }

    private char? NextOctalDigest()
    {
        var peeked = _state.Peek();
        if (!peeked.IsEoF())
        {
            if (char.IsDigit(peeked))
            {
                return _state.Next();
            }
        }

        return null;
    }

    private char? NextHexDigit()
    {
        if (_state.Peek().IsHex())
        {
            return _state.Next();
        }

        return null;
    }

    private static char? ByteToChar(string value, int radix)
    {
        try
        {
            // u32 is used here because Pg has an overflow operation rather than throwing an exception directly.
            var numeric = FromStringRadix(value, radix);

            var n = numeric & 0xFF;
            if (n <= 127)
            {
                return char.ConvertFromUtf32(n)[0];
            }

            return null;
        }
        catch
        {
            return null;
        }
    }
}
