using SqlParser.Dialects;
using SqlParser.Tokens;

namespace SqlParser;

public ref struct Tokenizer
{
    private Dialect _dialect;
    private State _state;
    private readonly bool _unescape;

    public Tokenizer(bool unescape = true)
    {
        _unescape = unescape;
    }

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

    private IList<Token> TokenizeWithLocation(ReadOnlySpan<char> sql)
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

            Symbols.SingleQuote => new SingleQuotedString(new string(TokenizeQuotedString(Symbols.SingleQuote))),
            Symbols.DoubleQuote when
                !_dialect.IsDelimitedIdentifierStart(character) &&
                !_dialect.IsIdentifierStart(character)
                    => new DoubleQuotedString(new string(TokenizeQuotedString(Symbols.DoubleQuote))),

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
            Symbols.Ampersand => TokenizeAmpersand(), //TokenizeSingleCharacter(new Ampersand()),
            Symbols.Caret => TokenizeSingleCharacter(new Caret()),
            Symbols.CurlyBracketOpen => TokenizeSingleCharacter(new LeftBrace()),
            Symbols.CurlyBracketClose => TokenizeSingleCharacter(new RightBrace()),

            Symbols.Num when _dialect is SnowflakeDialect => TokenizeSnowflakeComment(),
            Symbols.Tilde => TokenizeTilde(),
            Symbols.Num => TokenizeHash(character),
            Symbols.At => TokenizeAt(character),
            Symbols.QuestionMark => TokenizeQuestionMark(),
            // Identifier or keyword
            _ when _dialect.IsIdentifierStart(character) => TokenizeIdentifierOrKeyword(new []{ character }),
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

    private Token TokenizeCarriageReturn()
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
            Symbols.SingleQuote => new NationalStringLiteral(new string(TokenizeQuotedString(Symbols.SingleQuote))),
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

        var word = ConcatArrays(new[] { first }, TokenizeWord());
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

        var hex = TokenizeQuotedString(Symbols.SingleQuote);
        return new HexStringLiteral(new string(hex));

    }

    //private Token TokenizeIdent()
    //{
    //    var chars = TokenizeWord();

    //    // Check for numbers given dialects like Hive support
    //    // numeric identifiers
    //    if (!chars.All(c => c.IsDigit() || c == Symbols.Dot))
    //    {
    //        return new Word(new string(chars), null);
    //    }

    //    // Dialect supports digit identifier. Capture the remainder of the whole word
    //    // with all digits and dots. 
    //    var additional = _state.PeekTakeWhile(c => c.IsDigit() || c == Symbols.Dot);

    //    if (additional.Length > 0)
    //    {
    //        chars = ConcatArrays(chars, additional);
    //    }

    //    return new Number(new string(chars), false);
    //}

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

    private char[] TokenizeQuotedString(char quoteStyle)
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

                    if (!_unescape)
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
            else if (current == Symbols.Backslash)
            {
                _state.Next();

                if (_dialect is MySqlDialect)
                {
                    var next = _state.Peek();
                    if (!_unescape)
                    {
                        word.Add(current);
                        word.Add(next);
                        _state.Next();
                    }
                    else
                    {
                        var symbol = next switch
                        {
                            Symbols.SingleQuote
                                or Symbols.DoubleQuote 
                                or Symbols.Backslash 
                                or Symbols.Percent
                                or Symbols.Underscore
                                => next,
                            '0' => Symbols.Null,
                            'b' => Symbols.Backspace,
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
                    word.Add(current);
                }

                //_state.Next();
            }
            else
            {
                _state.Next();
                word.Add(current);
            }
        }

        throw new TokenizeException($"Unterminated string literal. Expected {quoteStyle} after {errorLocation}", errorLocation);
    }

    private Token TokenizeDelimitedQuoted(char startQuote)
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

                number = ConcatArrays(number, exponent.ToArray());
            }
        }

        if (_dialect is MySqlDialect or HiveDialect && exponent.Count == 0)
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

        if (_dialect.IsIdentifierStart(Symbols.Percent))
        {
            return TokenizeIdentifierOrKeyword(new[]{ character, next });
        }

        return new Modulo();
    }

    private Token TokenizeByteStringLiteral()
    {
        var current = _state.Peek();
        _state.Next();
        return _state.Peek() switch
        {
            Symbols.SingleQuote => new SingleQuotedByteStringLiteral(new string(TokenizeQuotedString(Symbols.SingleQuote))),
            Symbols.DoubleQuote => new DoubleQuotedByteStringLiteral(new string(TokenizeQuotedString(Symbols.DoubleQuote))),
            _ => new Word(new string(TokenizeWord(current)), null)
        };
    }

    private Token TokenizeRawStringLiteral()
    {
        var current = _state.Peek();
        _state.Next();
        return _state.Peek() switch
        {
            Symbols.SingleQuote => new RawStringLiteral(new string(TokenizeQuotedString(Symbols.SingleQuote))),
            Symbols.DoubleQuote => new RawStringLiteral(new string(TokenizeQuotedString(Symbols.DoubleQuote))),
            _ => new Word(new string(TokenizeWord(current)), null)
        };
    }

    /// Read a single quoted string, starting with the opening quote.
    private string TokenizeEscapedSingleQuotedString(Location start)
    {
        var s = new List<char>();
        _state.Next();
        // Slash escaping
        var isEscaped = false;
        char current;

        while ((current = _state.Peek()) != Symbols.EndOfFile)
        {
            switch (current)
            {
                case Symbols.SingleQuote:
                    var (escaped, continueEscape) = EscapeSingleQuote(s, current, isEscaped);
                    isEscaped = continueEscape;

                    if (escaped)
                    {
                        return new string(s.ToArray());
                    }
                    break;

                case Symbols.Backslash:
                    isEscaped = EscapeBackslash(s, isEscaped);
                    break;

                case 'r':
                    isEscaped = EscapeControlCharacter(s, current, isEscaped, Symbols.CarriageReturn);
                    break;
                case 'n':
                    isEscaped = EscapeControlCharacter(s, current, isEscaped, Symbols.NewLine);
                    break;
                case 't':
                    isEscaped = EscapeControlCharacter(s, current, isEscaped, Symbols.Tab);
                    break;

                default:
                    isEscaped = false;
                    _state.Next();
                    s.Add(current);
                    break;

            }
        }

        throw new TokenizeException($"Unterminated encoded string literal after {start}", start);
    }

    private (bool, bool) EscapeSingleQuote(ICollection<char> s, char current, bool isEscaped)
    {
        _state.Next();
        if (isEscaped)
        {
            s.Add(current);
            return (false, false);
        }

        if (_state.Peek() == Symbols.SingleQuote)
        {
            s.Add(current);
            return (false, false);
        }

        return (true, isEscaped);
    }

    private bool EscapeBackslash(ICollection<char> s, bool isEscaped)
    {
        bool escaped;

        if (isEscaped)
        {
            s.Add(Symbols.Backslash);
            escaped = false;
        }
        else
        {
            escaped = true;
        }

        _state.Next();
        return escaped;
    }

    private bool EscapeControlCharacter(ICollection<char> s, char current, bool isEscaped, char escaped)
    {
        var continueEscaped = isEscaped;

        if (isEscaped)
        {
            s.Add(escaped);
            continueEscaped = false;
        }
        else
        {
            s.Add(current);
        }
        _state.Next();

        return continueEscaped;
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
            comment = ConcatArrays(comment, new[] { Symbols.NewLine });
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
        //return _state.Peek() == Symbols.GreaterThan
        //    ? TokenizeSingleCharacter(new RightArrow())
        //    : new Equal();
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
                return _state.Peek() == Symbols.Asterisk
                    ? TokenizeSingleCharacter(new ExclamationMarkTildeAsterisk())
                    : new ExclamationMarkTilde();

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
            Symbols.Equal => TokenizeSingleCharacter(new DuckAssignment()),
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

    private Token TokenizeSnowflakeComment()
    {
        _state.Next();
        var comment = TokenizeInlineComment();

        return new Whitespace(WhitespaceKind.InlineComment, new string(comment))
        {
            Prefix = Symbols.Num.ToString()
        };
    }

    private Token TokenizeTilde()
    {
        _state.Next();
        return _state.Peek() == Symbols.Asterisk
            ? TokenizeSingleCharacter(new TildeAsterisk())
            : new Tilde();
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
                if (_dialect.IsIdentifierStart(Symbols.Num))
                {
                    return TokenizeIdentifierOrKeyword(new[] { character, _state.Peek() });
                }

                return new Hash();
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
            _ when _dialect.IsIdentifierStart(Symbols.At) => TokenizeIdentifierOrKeyword(new[]{ character, _state.Peek() }),
            _ => new AtSign()
        };
    }

    Token TokenizeAtAt(char character)
    {
        _state.Next();
        var next = _state.Peek();
        if (next is Symbols.Space)
        {
            return new AtAt();
        }

        return _dialect.IsIdentifierStart(Symbols.At) ? 
            TokenizeIdentifierOrKeyword(new[] { character, Symbols.At, next }) 
            : new AtAt();
    }

    private Token TokenizeQuestionMark()
    {
        _state.Next();
        var word = _state.PeekTakeWhile(c => c.IsDigit());
        var question = ConcatArrays(new[] { Symbols.QuestionMark }, word);
        return new Placeholder(new string(question));
    }

    private Token TokenizeDollar()
    {
        var s = new List<char>();
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

                    s.Add(Symbols.Dollar);
                    s.Add(current);
                }
                else if (current != Symbols.Dollar)
                {
                    s.Add(current);
                }

                prev = current;
                _state.Next();
            }

            if (_state.Peek() == Symbols.EndOfFile && !isTerminated)
            {
                throw new TokenizeException("Unterminated dollar-quoted string", _state.CloneLocation());
            }

            return new DollarQuotedString(new string(s.ToArray()));
        }

        // Placeholders can be any character in the broader unicode range,
        // not just ASCII letters and numbers.  As such, the tokenizer needs
        // to read every unicode alphanumeric character, hence the use of
        // the built in IsLetterOrDigit method.
        var word = _state.PeekTakeWhile(c => char.IsLetterOrDigit(c) || c == Symbols.Underscore);

        value.AddRange(word);

        if (_state.Peek() == Symbols.Dollar)
        {
            _state.Next();
            s.AddRange(_state.PeekTakeWhile(c => c != Symbols.Dollar));

            switch (_state.Peek())
            {
                case Symbols.Dollar:
                    _state.Next();

                    foreach (var _ in value)
                    {
                        _state.Next();
                        var next = _state.Peek();

                        if (next == Symbols.EndOfFile)
                        {
                            throw new TokenizeException($"Unterminated dollar-quoted string at or near {new string(value.ToArray())}", _state.CloneLocation());
                        }
                    }

                    if (_state.Peek() == Symbols.Dollar)
                    {
                        _state.Next();
                    }
                    else
                    {
                        throw new TokenizeException("Unterminated dollar-quoted, expected $", _state.CloneLocation());
                    }

                    break;

                default:
                    throw new TokenizeException("Unterminated dollar-quoted, expected $", _state.CloneLocation());
            }
        }
        else
        {
            return new Placeholder(new string(ConcatArrays(new[] { Symbols.Dollar }, value.ToArray())));
        }

        var quoted = new DollarQuotedString(new string(s.ToArray()));
        if (value.Any())
        {
            quoted.Tag = new string(value.ToArray());
        }

        return quoted;
    }

    private (string, char?) ParseQuotedIdent(char quoteEnd)
    {
        char? lastChar = null;
        char current;
        var chars = new List<char>();

        do
        {
            current = _state.Peek();
            _state.Next();

            if (current == quoteEnd)
            {
                if (_state.Peek() == quoteEnd)
                {
                    _state.Next();
                    chars.Add(current);

                    if (!_unescape)
                    {
                        chars.Add(current);
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
                chars.Add(current);
            }

        } while (current != Symbols.EndOfFile);

        return (new string(chars.ToArray()), lastChar);
    }
}