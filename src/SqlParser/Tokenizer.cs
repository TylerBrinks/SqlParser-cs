using System.Diagnostics.CodeAnalysis;
using System.Text;
using SqlParser.Dialects;
using SqlParser.Tokens;

namespace SqlParser;

public ref struct Tokenizer(bool unescape = true)
{
    private Dialect _dialect;
    private State _state;

    /// <summary>
    /// Reads a string into primitive SQL tokens using a generic SQL dialect
    /// </summary>
    /// <param name="sql">SQL string to tokenize</param>
    /// <returns>IEnumerable list of SQL tokens</returns>
    /// <exception cref="TokenizeException">Thrown when an unexpected token in encountered while parsing the input string</exception>
    public IList<Token> Tokenize(ReadOnlySpan<char> sql)
    {
        return Tokenize(sql, new GenericDialect());
    }
    /// <summary>
    ///  Reads a string into primitive SQL tokens using a given SQL dialect
    /// </summary>
    /// <param name="sql">SQL string to tokenize</param>
    /// <param name="dialect">SQL dialect</param>
    /// <returns>IEnumerable list of SQL tokens</returns>
    /// <exception cref="TokenizeException">Thrown when an unexpected token in encountered while parsing the input string</exception>
    public IList<Token> Tokenize(ReadOnlySpan<char> sql, Dialect dialect)
    {
        _dialect = dialect;
        return TokenizeWithLocation(sql);
    }

    private List<Token> TokenizeWithLocation(ReadOnlySpan<char> sql)
    {
        _state = new State(sql);

        var tokens = new List<Token>();
        Token token;
        var location = _state.CloneLocation();

        while ((token = NextToken()) is not EOF)
        {
            token.SetLocation(location);
            tokens.Add(token);
            location = _state.CloneLocation();
        }

        return tokens;
    }

    private Token NextToken()
    {
        var character = _state.Peek();

        return character switch
        {
            Symbols.Space => TokenizeSingleCharacter(new Whitespace(WhitespaceKind.Space)),
            Symbols.Tab => TokenizeSingleCharacter(new Whitespace(WhitespaceKind.Tab)),
            Symbols.NewLine => TokenizeSingleCharacter(new Whitespace(WhitespaceKind.NewLine)),
            Symbols.CarriageReturn => TokenizeCarriageReturn(),
            // BigQuery uses b or B for byte string literal
            'B' or 'b' when _dialect is BigQueryDialect or GenericDialect => TokenizeByteStringLiteral(),
            // BigQuery uses r or R for raw string literal
            'R' or 'r' when _dialect is BigQueryDialect or GenericDialect => TokenizeRawStringLiteral(),
            // Redshift uses lower case n for national string literal
            'N' or 'n' => TokenizeNationalStringLiteral(character),
            // PostgreSQL accepts "escape" string constants, which are an extension to the SQL standard.
            'E' or 'e' => TokenizeEscape(character),
            // The spec only allows an uppercase 'X' to introduce a hex
            // string, but PostgreSQL, at least, allows a lowercase 'x' too.
            'X' or 'x' => TokenizeHex(),

            Symbols.SingleQuote => new SingleQuotedString(
                new string(TokenizeQuotedString(Symbols.SingleQuote, _dialect.SupportsStringLiteralBackslashEscape))),

            Symbols.DoubleQuote when
                !_dialect.IsDelimitedIdentifierStart(character) &&
                !_dialect.IsIdentifierStart(character) => new DoubleQuotedString(
                    new string(TokenizeQuotedString(Symbols.DoubleQuote, _dialect.SupportsStringLiteralBackslashEscape))),

            // Delimited (quoted) identifier
            _ when
                _dialect.IsDelimitedIdentifierStart(character) &&
                _dialect.IsProperIdentifierInsideQuotes(_state.Clone())
                    => TokenizeDelimitedQuoted(character),

            _ when character.IsDigit() || character == Symbols.Dot => TokenizeNumber(),

            // Punctuation
            Symbols.ParenOpen => TokenizeSingleCharacter(new LeftParen()),
            Symbols.ParenClose => TokenizeSingleCharacter(new RightParen()),
            Symbols.Comma => TokenizeSingleCharacter(new Comma()),
            // Operators
            Symbols.Minus => TokenizeMinus(),
            Symbols.Divide => TokenizeDivide(),
            Symbols.Plus => TokenizeSingleCharacter(new Plus()),
            Symbols.Asterisk => TokenizeSingleCharacter(new Multiply()),
            Symbols.Percent => TokenizePercent(character),// TokenizeSingleCharacter(new Modulo()),

            Symbols.Pipe => TokenizePipe(),
            Symbols.Equal => TokenizeEqual(),
            Symbols.ExclamationMark => TokenizeExclamation(),
            Symbols.LessThan => TokenizeLessThan(),
            Symbols.GreaterThan => TokenizeGreaterThan(),
            Symbols.Colon => TokenizeColon(),

            Symbols.Semicolon => TokenizeSingleCharacter(new SemiColon()),
            Symbols.Backslash => TokenizeSingleCharacter(new Backslash()),
            Symbols.SquareBracketOpen => TokenizeSingleCharacter(new LeftBracket()),
            Symbols.SquareBracketClose => TokenizeSingleCharacter(new RightBracket()),
            Symbols.Ampersand => TokenizeAmpersand(),
            Symbols.Caret => TokenizeCaret(),
            Symbols.CurlyBracketOpen => TokenizeSingleCharacter(new LeftBrace()),
            Symbols.CurlyBracketClose => TokenizeSingleCharacter(new RightBrace()),

            Symbols.Num when _dialect is SnowflakeDialect or BigQueryDialect => TokenizeSnowflakeComment(),
            Symbols.Tilde => TokenizeTilde(),
            Symbols.Num => TokenizeHash(character),
            Symbols.At => TokenizeAt(character),
            Symbols.QuestionMark when _dialect is PostgreSqlDialect => TokenizePgQuestion(),

            Symbols.QuestionMark => TokenizeQuestionMark(),
            // Identifier or keyword
            _ when _dialect.IsIdentifierStart(character) => TokenizeIdentifierOrKeyword([character]),
            Symbols.Dollar => TokenizeDollar(),
            _ when string.IsNullOrWhiteSpace(character.ToString()) => TokenizeSingleCharacter(new Whitespace(WhitespaceKind.Space)),
            // Unknown character
            _ when character != Symbols.EndOfFile => TokenizeSingleCharacter(new SingleCharacterToken(character)),
            _ => new EOF()
        };
    }

    public static char[] ConcatArrays(params char[][] arrays)
    {
        var first = arrays[0];
        var list = arrays.Skip(1)
            .Aggregate<char[]?, IEnumerable<char>>(first, (current, next) => current.Concat(next!));
        return list.ToArray();
    }

    private Token TokenizeSingleCharacter(Token token)
    {
        _state.Next();
        return token;
    }

    private Whitespace TokenizeCarriageReturn()
    {
        _state.Next();
        if (_state.Peek() == Symbols.NewLine)
        {
            _state.Next();
        }

        return new Whitespace(WhitespaceKind.NewLine);
    }

    private Token TokenizeNationalStringLiteral(char first)
    {
        _state.Next();
        return _state.Peek() switch
        {
            Symbols.SingleQuote => new NationalStringLiteral(new string(TokenizeQuotedString(Symbols.SingleQuote, true))),
            _ => new Word(new string(TokenizeWord(first)), null)
        };
    }

    private Token TokenizeEscape(char first)
    {
        var start = _state.CloneLocation();
        _state.Next();

        var next = _state.Peek();
        if (next == Symbols.SingleQuote)
        {
            var quoted = TokenizeEscapedSingleQuotedString(start);
            return new EscapedStringLiteral(quoted);
        }

        var word = ConcatArrays([first], TokenizeWord());
        return new Word(new string(word), null);
    }

    private Token TokenizeHex()
    {
        var first = _state.Peek();
        _state.Next();

        if (_state.Peek() != Symbols.SingleQuote)
        {
            return new Word(new string(TokenizeWord(first)), null);
        }

        var hex = TokenizeQuotedString(Symbols.SingleQuote, true);
        return new HexStringLiteral(new string(hex));

    }

    public Token TokenizeIdentifierOrKeyword(char[] characters)
    {
        _state.Next();
        var word = TokenizeWord(characters);

        if (word.All(c => c.IsDigit() || c == Symbols.Dot))
        {
            var innerState = new State(word);

            var s = innerState.PeekTakeWhile(w => w.IsDigit() || w is Symbols.Dot);
            var s2 = _state.PeekTakeWhile(ch => ch.IsDigit() || ch is Symbols.Dot);
            var number = s.Concat(s2);

            return new Number(new string(number.ToArray()));
        }

        return new Word(new string(word));
    }

    private char[] TokenizeWord(params char[]? first)
    {
        var prefix = new List<char>();
        if (first != null)
        {
            prefix.AddRange(first);
        }

        var word = _state.PeekTakeWhile(_dialect.IsIdentifierPart);

        return prefix.Count != 0
            ? ConcatArrays([.. prefix], word)
            : word;
    }

    private char[] TokenizeQuotedString(char quoteStyle, bool allowEscape)
    {
        var errorLocation = _state.CloneLocation();
        var word = new List<char>();

        // Consume the quote
        _state.Next();

        char current;

        while ((current = _state.Peek()) != Symbols.EndOfFile)
        {
            if (quoteStyle == current)
            {
                _state.Next();

                if (_state.Peek() == quoteStyle)
                {
                    word.Add(current);

                    if (!unescape)
                    {
                        // In no-escape mode, the given query has to be saved completely
                        word.Add(current);
                    }

                    _state.Next();
                }
                else
                {
                    return word.ToArray();
                }

            }
            else if (current == Symbols.Backslash && allowEscape)
            {
                _state.Next();

                var next = _state.Peek();
                if (!unescape)
                {
                    word.Add(current);
                    word.Add(next);
                    _state.Next();
                }
                else
                {
                    var symbol = next switch
                    {
                        '0' => Symbols.Null,
                        'a' => Symbols.Bel,
                        'b' => Symbols.Backspace,
                        'f' => Symbols.FormFeed,
                        'n' => Symbols.NewLine,
                        'r' => Symbols.CarriageReturn,
                        't' => Symbols.Tab,
                        'Z' => Symbols.Sub,
                        _ => next
                    };
                    word.Add(symbol);
                    _state.Next();
                }
            }
            else
            {
                _state.Next();
                word.Add(current);
            }
        }

        throw new TokenizeException($"Unterminated string literal. Expected {quoteStyle} after {errorLocation}", errorLocation);
    }

    private Word TokenizeDelimitedQuoted(char startQuote)
    {
        var errorLocation = _state.CloneLocation();

        _state.Next(); // Consume the opening quote

        var quoteEnd = Word.GetEndQuote(startQuote);

        var (word, lastChar) = ParseQuotedIdent(quoteEnd);

        if (lastChar == quoteEnd)
        {
            return new Word(word, startQuote);
        }

        throw new TokenizeException($"Expected close delimiter '{quoteEnd}' before EOF.", errorLocation);

    }

    private Token TokenizeNumber()
    {
        var parsed = new List<char>(_state.PeekTakeWhile(c => c.IsDigit()));

        // match binary literal that starts with 0x
        if (parsed is [Symbols.Zero] && _state.Peek() == 'x')
        {
            _state.Next();

            var hex = _state.PeekTakeWhile(c => c.IsHex());
            return new HexStringLiteral(new string(hex));
        }

        // match one period
        if (_state.Peek() == Symbols.Dot)
        {
            parsed.Add(Symbols.Dot);
            _state.Next();
        }
        parsed.AddRange(_state.PeekTakeWhile(c => c.IsDigit()));

        // No number -> Token::Period
        if (parsed is [Symbols.Dot])
        {
            return new Period();
        }
        var number = parsed.ToArray();

        var exponent = new List<char>();

        // Parse exponent as number
        if (_state.Peek() is 'e' or 'E')
        {
            var exponentState = _state.Clone();
            exponent.Add(exponentState.Peek());
            exponentState.Next();

            var next = exponentState.Peek();
            if (next is Symbols.Plus or Symbols.Minus)
            {
                exponent.Add(next);
                exponentState.Next();
            }

            if (exponentState.Peek().IsDigit())
            {
                for (var i = 0; i < exponent.Count; i++)
                {
                    // advance the original state to the location where the exponent 
                    // notation ends and the numeric values continue
                    _state.Next();
                }

                var exponentNumber = _state.PeekTakeWhile(c => c.IsDigit());
                exponent.AddRange(exponentNumber);

                number = ConcatArrays(number, [.. exponent]);
            }
        }

        if (_dialect.SupportsNumericPrefix && exponent.Count == 0)
        {
            var word = _state.PeekTakeWhile(_dialect.IsIdentifierPart);

            if (word.Length > 0)
            {
                parsed.AddRange(word);
                return new Word(new string(parsed.ToArray()));
            }
        }

        var @long = false;
        if (_state.Peek() == 'L')
        {
            @long = true;
            _state.Next();
        }

        return new Number(new string(number.ToArray()), @long);
    }

    private Token TokenizePercent(char character)
    {
        _state.Next();
        var next = _state.Peek();

        if (next is Symbols.Space)
        {
            return new Modulo();
        }

        return _dialect.IsIdentifierStart(Symbols.Percent) ? TokenizeIdentifierOrKeyword([character, next]) : new Modulo();
    }

    private Token TokenizeByteStringLiteral()
    {
        var current = _state.Peek();
        _state.Next();
        return _state.Peek() switch
        {
            Symbols.SingleQuote => new SingleQuotedByteStringLiteral(new string(TokenizeQuotedString(Symbols.SingleQuote, false))),
            Symbols.DoubleQuote => new DoubleQuotedByteStringLiteral(new string(TokenizeQuotedString(Symbols.DoubleQuote, false))),
            _ => new Word(new string(TokenizeWord(current)), null)
        };
    }

    private Token TokenizeRawStringLiteral()
    {
        var current = _state.Peek();
        _state.Next();
        return _state.Peek() switch
        {
            Symbols.SingleQuote => new RawStringLiteral(new string(TokenizeQuotedString(Symbols.SingleQuote, false))),
            Symbols.DoubleQuote => new RawStringLiteral(new string(TokenizeQuotedString(Symbols.DoubleQuote, false))),
            _ => new Word(new string(TokenizeWord(current)), null)
        };
    }

    /// Read a single quoted string, starting with the opening quote.
    private string TokenizeEscapedSingleQuotedString(Location start)
    {
        var unescaped = UnescapeSingleQuotedString();

        if (unescaped != null)
        {
            return unescaped;
        }

        throw new TokenizeException("Unterminated encoded string literal", start);
    }

    private string? UnescapeSingleQuotedString()
    {
        return new Unescaper(ref _state).Unescape();
    }

    private Token TokenizeMinus()
    {
        _state.Next();
        return _state.Peek() switch
        {
            // -- inline comment
            Symbols.Minus => TokenizeHyphenComment(),
            // -> or ->> long arrow
            Symbols.GreaterThan => TokenizeLongArrow(),
            // Regular - operator
            _ => new Minus()
        };
    }

    private Whitespace TokenizeHyphenComment()
    {
        _state.Next();
        var comment = TokenizeInlineComment();
        return new Whitespace(WhitespaceKind.InlineComment, new string(comment)) { Prefix = "--" };
    }

    private Token TokenizeLongArrow()
    {
        _state.Next();
        if (_state.Peek() != Symbols.GreaterThan)
        {
            return new Arrow();
        }

        _state.Next();
        return new LongArrow();
    }

    private Token TokenizeDivide()
    {
        _state.Next();

        return _state.Peek() switch
        {
            Symbols.Asterisk => TokenizeAsteriskComment(),
            Symbols.Divide when _dialect is SnowflakeDialect => ParseSnowflakeComment(),
            Symbols.Divide when _dialect is DuckDbDialect or GenericDialect => ParseIntDiv(_state),
            _ => new Divide()
        };

        static Token ParseIntDiv(State state)
        {
            state.Next();
            return new DuckIntDiv();
        }
    }

    private Whitespace TokenizeAsteriskComment()
    {
        var comment = new Stack<char>();
        var lastChar = Symbols.Space;
        var nested = 1;

        while (true)
        {
            _state.Next();

            var current = _state.Peek();

            if (current == Symbols.EndOfFile)
            {
                throw new TokenizeException("Unexpected EOF while in a multi-line comment", _state.CloneLocation());
            }

            switch (lastChar)
            {
                case Symbols.Divide when current == Symbols.Asterisk:
                    nested++;
                    break;

                case Symbols.Asterisk when current == Symbols.Divide:
                    {
                        nested--;
                        if (nested == 0)
                        {
                            comment.Pop();
                            _state.Next();
                            return new Whitespace(WhitespaceKind.MultilineComment, new string(comment.Reverse().ToArray()));
                        }

                        break;
                    }
            }

            comment.Push(current);
            lastChar = current;
        }
    }

    private Whitespace ParseSnowflakeComment()
    {
        _state.Next();
        var comment = TokenizeInlineComment();
        return new Whitespace(WhitespaceKind.InlineComment, new string(comment))
        {
            Prefix = "//"
        };
    }

    private char[] TokenizeInlineComment()
    {
        var comment = _state.PeekTakeWhile(c => c != Symbols.NewLine);

        if (_state.Peek() == Symbols.NewLine)
        {
            comment = ConcatArrays(comment, [Symbols.NewLine]);
            _state.Next();
        }

        return comment;
    }

    private Token TokenizePipe()
    {
        _state.Next();
        switch (_state.Peek())
        {
            case Symbols.Divide:
                return TokenizeSingleCharacter(new PGSquareRoot());

            case Symbols.Pipe:
                _state.Next();
                return _state.Peek() == Symbols.Divide ? TokenizeSingleCharacter(new PGCubeRoot()) : new StringConcat();

            default:
                return new Pipe();
        }
    }

    private Token TokenizeEqual()
    {
        _state.Next();
        var next = _state.Peek();
        return next switch
        {
            Symbols.GreaterThan => TokenizeSingleCharacter(new RightArrow()),
            Symbols.Equal => TokenizeSingleCharacter(new DoubleEqual()),
            _ => new Equal()
        };
    }

    private Token TokenizeExclamation()
    {
        _state.Next();
        switch (_state.Peek())
        {
            case Symbols.Equal:
                return TokenizeSingleCharacter(new NotEqual());

            case Symbols.ExclamationMark:
                return TokenizeSingleCharacter(new DoubleExclamationMark());

            case Symbols.Tilde:
                _state.Next();
                var peek = _state.Peek();
                switch (peek)
                {
                    case Symbols.Asterisk:
                        return TokenizeSingleCharacter(new ExclamationMarkTildeAsterisk());

                    case Symbols.Tilde:
                        _state.Next();
                        return _state.Peek() == Symbols.Asterisk ?
                            TokenizeSingleCharacter(new ExclamationMarkDoubleTildeAsterisk())
                            : new ExclamationMarkDoubleTilde();

                    default:
                        return new ExclamationMarkTilde();
                }

            default:
                return new ExclamationMark();
        }
    }

    private Token TokenizeLessThan()
    {
        _state.Next();
        switch (_state.Peek())
        {
            case Symbols.Equal:
                _state.Next();
                return _state.Peek() == Symbols.GreaterThan ? TokenizeSingleCharacter(new Spaceship()) : new LessThanOrEqual();

            case Symbols.GreaterThan:
                return TokenizeSingleCharacter(new NotEqual());
            case Symbols.LessThan:
                return TokenizeSingleCharacter(new ShiftLeft());
            case Symbols.At:
                return TokenizeSingleCharacter(new ArrowAt());
            default:
                return new LessThan();
        }
    }

    private Token TokenizeGreaterThan()
    {
        _state.Next();
        return _state.Peek() switch
        {
            Symbols.Equal => TokenizeSingleCharacter(new GreaterThanOrEqual()),
            Symbols.GreaterThan => TokenizeSingleCharacter(new ShiftRight()),
            _ => new GreaterThan()
        };
    }

    private Token TokenizeColon()
    {
        _state.Next();
        var token = _state.Peek();

        return token switch
        {
            Symbols.Colon => TokenizeSingleCharacter(new DoubleColon()),
            Symbols.Equal => TokenizeSingleCharacter(new Assignment()),
            _ => new Colon()
        };
    }

    private Token TokenizeAmpersand()
    {
        _state.Next();
        var token = _state.Peek();

        if (token is not Symbols.Ampersand)
        {
            return new Ampersand();
        }

        _state.Next();
        return new Overlap();
    }

    private Token TokenizeCaret()
    {
        _state.Next();
        var token = _state.Peek();

        return token is Symbols.At ? TokenizeSingleCharacter(new CaretAt()) : new Caret();
    }

    private Whitespace TokenizeSnowflakeComment()
    {
        _state.Next();
        var comment = TokenizeInlineComment();

        return new Whitespace(WhitespaceKind.InlineComment, new string(comment))
        {
            Prefix = Symbols.Num.ToString()
        };
    }

    private Token TokenizePgQuestion()
    {
        _state.Next();
        return _state.Peek() switch
        {
            Symbols.Pipe => TokenizeSingleCharacter(new QuestionPipe()),
            Symbols.Ampersand => TokenizeSingleCharacter(new QuestionAnd()),
            _ => TokenizeSingleCharacter(new Question()),
        };
    }

    private Token TokenizeTilde()
    {
        _state.Next();
        var peek = _state.Peek();

        switch (peek)
        {
            case Symbols.Asterisk:
                return TokenizeSingleCharacter(new TildeAsterisk());

            case Symbols.Tilde:
                _state.Next();
                return _state.Peek() == Symbols.Asterisk
                    ? TokenizeSingleCharacter(new DoubleTildeAsterisk())
                    : new DoubleTilde();

            default:
                return new Tilde();
        }
    }

    private Token TokenizeHash(char character)
    {
        _state.Next();
        switch (_state.Peek())
        {
            case Symbols.Minus:
                return TokenizeSingleCharacter(new HashMinus());

            case Symbols.GreaterThan:
                _state.Next();
                if (_state.Peek() != Symbols.GreaterThan)
                {
                    return new HashArrow();
                }

                _state.Next();
                return new HashLongArrow();

            case Symbols.Space:
                return new Hash();

            default:
                return _dialect.IsIdentifierStart(Symbols.Num) ? TokenizeIdentifierOrKeyword([character, _state.Peek()]) : new Hash();
        }
    }

    private Token TokenizeAt(char character)
    {
        _state.Next();
        return _state.Peek() switch
        {
            Symbols.GreaterThan => TokenizeSingleCharacter(new AtArrow()),
            Symbols.QuestionMark => TokenizeSingleCharacter(new AtQuestion()),
            Symbols.At => TokenizeAtAt(character),
            Symbols.Space => new AtSign(),
            _ when _dialect.IsIdentifierStart(Symbols.At) => TokenizeIdentifierOrKeyword([character, _state.Peek()]),
            _ => new AtSign()
        };
    }

    private Token TokenizeAtAt(char character)
    {
        _state.Next();
        var next = _state.Peek();
        if (next is Symbols.Space)
        {
            return new AtAt();
        }

        return _dialect.IsIdentifierStart(Symbols.At) ?
            TokenizeIdentifierOrKeyword([character, Symbols.At, next])
            : new AtAt();
    }

    private Placeholder TokenizeQuestionMark()
    {
        _state.Next();
        var word = _state.PeekTakeWhile(c => c.IsDigit());
        var question = ConcatArrays([Symbols.QuestionMark], word);
        return new Placeholder(new string(question));
    }

    private Token TokenizeDollar()
    {
        var builder = new StringBuilder();
        var value = new List<char>();

        _state.Next();

        if (_state.Peek() == Symbols.Dollar)
        {
            _state.Next();
            var isTerminated = false;
            char? prev = null;
            char current;

            while ((current = _state.Peek()) != Symbols.EndOfFile)
            {
                if (prev == Symbols.Dollar)
                {
                    if (current == Symbols.Dollar)
                    {
                        _state.Next();
                        isTerminated = true;
                        break;
                    }

                    builder.Append(Symbols.Dollar);
                    builder.Append(current);
                }
                else if (current != Symbols.Dollar)
                {
                    builder.Append(current);
                }

                prev = current;
                _state.Next();
            }

            if (_state.Peek() == Symbols.EndOfFile && !isTerminated)
            {
                throw new TokenizeException("Unterminated dollar-quoted string", _state.CloneLocation());
            }

            return new DollarQuotedString(builder.ToString());
        }

        // Placeholders can be any character in the broader unicode range,
        // not just ASCII letters and numbers.  As such, the tokenizer needs
        // to read every unicode alphanumeric character, hence the use of
        // the built-in IsLetterOrDigit method.
        var word = _state.PeekTakeWhile(c => char.IsLetterOrDigit(c) || c == Symbols.Underscore);

        value.AddRange(word);

        if (_state.Peek() == Symbols.Dollar)
        {
            _state.Next();
            var loop = true;
            while (loop)
            {
                var range = _state.PeekTakeWhile(c => c != Symbols.Dollar);
                foreach (var r in range)
                {
                    builder.Append(r);
                }

                var breakToLoop = false;

                switch (_state.Peek())
                {
                    case Symbols.Dollar:
                        _state.Next();
                        var intermediate = new StringBuilder("$");

                        foreach (var c in value)
                        {
                            var nextChar = _state.Next();
                            if (nextChar != null)
                            {
                                intermediate.Append(nextChar);
                                if (nextChar != c)
                                {
                                    builder.Append(intermediate.ToString());
                                    breakToLoop = true;
                                    break;
                                }
                            }
                            else
                            {
                                throw new TokenizeException(
                                    $"Unterminated dollar-quoted string at or near {new string(value.ToArray())}",
                                    _state.CloneLocation());
                            }
                        }

                        if (breakToLoop)
                        {
                            continue;
                        }

                        if (_state.Peek() == Symbols.Dollar)
                        {
                            _state.Next();
                            intermediate.Append("$");
                            loop = false;
                            break;
                        }

                        builder.Append(intermediate.ToString());
                        continue;

                    default:
                        throw new TokenizeException("Unterminated dollar-quoted, expected $", _state.CloneLocation());
                }
            }
        }
        else
        {
            return new Placeholder(new string(ConcatArrays([Symbols.Dollar], [.. value])));
        }

        var quoted = new DollarQuotedString(builder.ToString());
        if (value.Count != 0)
        {
            quoted.Tag = new string(value.ToArray());
        }

        return quoted;
    }

    private (string, char?) ParseQuotedIdent(char quoteEnd)
    {
        char? lastChar = null;
        char current;
        var chars = new StringBuilder();

        do
        {
            current = _state.Peek();
            _state.Next();

            if (current == quoteEnd)
            {
                if (_state.Peek() == quoteEnd)
                {
                    _state.Next();
                    chars.Append(current);

                    if (!unescape)
                    {
                        chars.Append(current);
                    }
                }
                else
                {
                    lastChar = quoteEnd;
                    break;
                }
            }
            else
            {
                chars.Append(current);
            }

        } while (current != Symbols.EndOfFile);

        return (chars.ToString(), lastChar);
    }
}