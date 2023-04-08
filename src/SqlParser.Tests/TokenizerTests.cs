using SqlParser.Dialects;
using SqlParser.Tokens;

namespace SqlParser.Tests
{
    public class TokenizerTests : TokenizerTestBase
    {
        [Fact]
        public void Tokenizer_Select_1()
        {
            var tokens = new Tokenizer().Tokenize("SELECT 1");

            var expected = new Token[]
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Number("1", false)
            };
            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenizer_Select_Float()
        {
            var tokens = new Tokenizer().Tokenize("SELECT .1");

            var expected = new Token[]
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Number(".1", false)
            };
            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenizer_Select_Exponent()
        {
            var tokens = new Tokenizer().Tokenize("SELECT 1e10, 1e-10, 1e+10, 1ea, 1e-10a, 1e-10-10");
            var expected = new Token[]
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Number("1e10"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Number("1e-10"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Number("1e+10"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Number("1"),
                new Word("ea"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Number("1e-10"),
                new Word("a"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Number("1e-10"),
                new Minus(),
                new Number("10")
            };
            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenizer_Scalar_Function()
        {
            var tokens = new Tokenizer().Tokenize("SELECT sqrt(1)");

            var expected = new Token[]
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Word("sqrt"),
                new LeftParen(),
                new Number("1"),
                new RightParen()
            };
            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenizer_String_String_Concat()
        {
            var tokens = new Tokenizer().Tokenize("SELECT 'a' || 'b'");

            var expected = new Token[]
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new SingleQuotedString("a"),
                new Whitespace(WhitespaceKind.Space),
                new StringConcat(),
                new Whitespace(WhitespaceKind.Space),
                new SingleQuotedString("b")
            };
            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Bitwise_Op()
        {
            var tokens = new Tokenizer().Tokenize("SELECT one | two ^ three");

            var expected = new Token[]
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Word("one"),
                new Whitespace(WhitespaceKind.Space),
                new Pipe(),
                new Whitespace(WhitespaceKind.Space),
                new Word("two"),
                new Whitespace(WhitespaceKind.Space),
                new Caret(),
                new Whitespace(WhitespaceKind.Space),
                new Word("three")
            };
            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Logical_Xor()
        {
            var tokens = new Tokenizer().Tokenize("SELECT true XOR true, false XOR false, true XOR false, false XOR true");

            var expected = new Token[]
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Word("true"),
                new Whitespace(WhitespaceKind.Space),
                new Word("XOR"),
                new Whitespace(WhitespaceKind.Space),
                new Word("true"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Word("false"),
                new Whitespace(WhitespaceKind.Space),
                new Word("XOR"),
                new Whitespace(WhitespaceKind.Space),
                new Word("false"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Word("true"),
                new Whitespace(WhitespaceKind.Space),
                new Word("XOR"),
                new Whitespace(WhitespaceKind.Space),
                new Word("false"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Word("false"),
                new Whitespace(WhitespaceKind.Space),
                new Word("XOR"),
                new Whitespace(WhitespaceKind.Space),
                new Word("true")
            };
            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Simple_Select()
        {
            var tokens = new Tokenizer().Tokenize("SELECT * FROM customer WHERE id = 1 LIMIT 5");

            var expected = new Token[]
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Multiply(),
                new Whitespace(WhitespaceKind.Space),
                new Word("FROM"),
                new Whitespace(WhitespaceKind.Space),
                new Word("customer"),
                new Whitespace(WhitespaceKind.Space),
                new Word("WHERE"),
                new Whitespace(WhitespaceKind.Space),
                new Word("id"),
                new Whitespace(WhitespaceKind.Space),
                new Equal(),
                new Whitespace(WhitespaceKind.Space),
                new Number("1"),
                new Whitespace(WhitespaceKind.Space),
                new Word("LIMIT"),
                new Whitespace(WhitespaceKind.Space),
                new Number("5")
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Explain_Select()
        {
            var tokens = new Tokenizer().Tokenize("EXPLAIN SELECT * FROM customer WHERE id = 1");
            var expected = new Token[]
            {
                new Word("EXPLAIN"),
                new Whitespace(WhitespaceKind.Space),
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Multiply(),
                new Whitespace(WhitespaceKind.Space),
                new Word("FROM"),
                new Whitespace(WhitespaceKind.Space),
                new Word("customer"),
                new Whitespace(WhitespaceKind.Space),
                new Word("WHERE"),
                new Whitespace(WhitespaceKind.Space),
                new Word("id"),
                new Whitespace(WhitespaceKind.Space),
                new Equal(),
                new Whitespace(WhitespaceKind.Space),
                new Number("1")
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Explain_Analyze_Select()
        {
            var tokens = new Tokenizer().Tokenize("EXPLAIN ANALYZE SELECT * FROM customer WHERE id = 1");

            var expected = new Token[]
            {
                new Word("EXPLAIN"),
                new Whitespace(WhitespaceKind.Space),
                new Word("ANALYZE"),
                new Whitespace(WhitespaceKind.Space),
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Multiply(),
                new Whitespace(WhitespaceKind.Space),
                new Word("FROM"),
                new Whitespace(WhitespaceKind.Space),
                new Word("customer"),
                new Whitespace(WhitespaceKind.Space),
                new Word("WHERE"),
                new Whitespace(WhitespaceKind.Space),
                new Word("id"),
                new Whitespace(WhitespaceKind.Space),
                new Equal(),
                new Whitespace(WhitespaceKind.Space),
                new Number("1")
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_String_Predicate()
        {
            var tokens = new Tokenizer().Tokenize("SELECT * FROM customer WHERE salary != 'Not Provided'");
            var expected = new Token[]
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Multiply(),
                new Whitespace(WhitespaceKind.Space),
                new Word("FROM"),
                new Whitespace(WhitespaceKind.Space),
                new Word("customer"),
                new Whitespace(WhitespaceKind.Space),
                new Word("WHERE"),
                new Whitespace(WhitespaceKind.Space),
                new Word("salary"),
                new Whitespace(WhitespaceKind.Space),
                new NotEqual(),
                new Whitespace(WhitespaceKind.Space),
                new SingleQuotedString("Not Provided")
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Invalid_String()
        {
            var symbol = '\ud83d';
            var sql = "\n\ud83dمصطفىh";

            var dialect = new GenericDialect();
            var tokens = new Tokenizer().Tokenize(sql, dialect);
            var expected = new List<Token>
            {
                new Whitespace(WhitespaceKind.NewLine),
                new SingleCharacterToken(symbol),
                new Word("مصطفىh")
            };
            Assert.Equal(expected, tokens);
        }

        [Fact]
        public void Tokenize_Newline_In_String_Literal()
        {
            var tokens = new Tokenizer().Tokenize("'foo\r\nbar\nbaz'");

            var expected = new Token[]
            {
                new SingleQuotedString("foo\r\nbar\nbaz")
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Unterminated_String_Literal()
        {
            var ex = Assert.Throws<TokenizeException>(() => new Tokenizer().Tokenize("select 'foo"));
            Assert.Equal("Unterminated string literal. Expected ' after Line: 1, Col: 8", ex.Message);
            Assert.Equal(1, ex.Line);
            Assert.Equal(8, ex.Column);
        }

        [Fact]
        public void Tokenize_Unterminated_String_Literal_UTF8()
        {
            var ex = Assert.Throws<TokenizeException>(() => new Tokenizer().Tokenize("SELECT \"なにか\" FROM Y WHERE \"なにか\" = 'test;"));

            Assert.Equal("Unterminated string literal. Expected ' after Line: 1, Col: 35", ex.Message);
            Assert.Equal(1, ex.Line);
            Assert.Equal(35, ex.Column);
        }

        [Fact]
        public void Tokenize_Invalid_String_Cols()
        {
            var symbol = '\ud83d';

            // ReSharper disable once StringLiteralTypo
            var tokens = new Tokenizer().Tokenize($"\n\nSELECT * FROM table\t{symbol}مصطفىh");

            var expected = new Token[]
            {
                new Whitespace(WhitespaceKind.NewLine),
                new Whitespace(WhitespaceKind.NewLine),
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Multiply(),
                new Whitespace(WhitespaceKind.Space),
                new Word("FROM"),
                new Whitespace(WhitespaceKind.Space),
                new Word("table"),
                new Whitespace(WhitespaceKind.Tab),
                new SingleCharacterToken(symbol),
                new Word("مصطفىh")
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Right_Arrow()
        {
            var tokens = new Tokenizer().Tokenize("FUNCTION(key=>value)");
            var expected = new Token[]
            {
                new Word("FUNCTION"),
                new LeftParen(),
                new Word("key"),
                new RightArrow(),
                new Word("value"),
                new RightParen()
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Is_Null()
        {
            var tokens = new Tokenizer().Tokenize("a IS NULL");
            var expected = new Token[]
            {
                new Word("a"),
                new Whitespace(WhitespaceKind.Space),
                new Word("IS"),
                new Whitespace(WhitespaceKind.Space),
                new Word("NULL"),
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Comment()
        {
            var tokens = new Tokenizer().Tokenize("0--this is a comment\n1");

            var expected = new Token[]
            {
                new Number("0"),
                new Whitespace(WhitespaceKind.InlineComment, "this is a comment\n")
                {
                    Prefix= "--"
                },
                new Number("1")
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Comment_At_EoF()
        {
            var tokens = new Tokenizer().Tokenize("--this is a comment");

            var expected = new Token[]
            {
                new Whitespace(WhitespaceKind.InlineComment, "this is a comment")
                {
                    Prefix= "--"
                }
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Multiline_Comment()
        {
            var tokens = new Tokenizer().Tokenize("0/*multi-line\n* /comment*/1");

            var expected = new Token[]
            {
                new Number("0"),
                new Whitespace(WhitespaceKind.MultilineComment, "multi-line\n* /comment"),
                new Number("1")
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_nested_multiline_comment()
        {
            var tokens = new Tokenizer().Tokenize("0/*multi-line\n* \n/* comment \n /*comment*/*/ */ /comment*/1");
            var expected = new Token[]
            {
                new Number("0"),
                new Whitespace(WhitespaceKind.MultilineComment, "multi-line\n* \n/* comment \n /*comment*/*/ */ /comment"),
                new Number("1")
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_multiline_comment_with_even_asterisks()
        {
            var tokens = new Tokenizer().Tokenize("\n/** Comment **/\n");

            var expected = new Token[]
            {
                new Whitespace(WhitespaceKind.NewLine),
                new Whitespace(WhitespaceKind.MultilineComment,"* Comment *"),
                new Whitespace(WhitespaceKind.NewLine)
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Unicode_Whitespace()
        {
            var tokens = new Tokenizer().Tokenize(" \u2003\n");

            var expected = new Token[]
            {
                new Whitespace(WhitespaceKind.Space),
                new Whitespace(WhitespaceKind.Space),
                new Whitespace(WhitespaceKind.NewLine)
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Mismatched_Quotes()
        {
            var ex = Assert.Throws<TokenizeException>(() => new Tokenizer().Tokenize("\"foo"));
            Assert.Equal("Expected close delimiter '\"' before EOF.", ex.Message);
            Assert.Equal(1, ex.Line);
            Assert.Equal(1, ex.Column);
        }

        [Fact]
        public void Tokenize_NewLines()
        {
            var tokens = new Tokenizer().Tokenize("line1\nline2\rline3\r\nline4\r");

            var expected = new Token[]
            {
                new Word("line1"),
                new Whitespace(WhitespaceKind.NewLine),
                new Word("line2"),
                new Whitespace(WhitespaceKind.NewLine),
                new Word("line3"),
                new Whitespace(WhitespaceKind.NewLine),
                new Word("line4"),
                new Whitespace(WhitespaceKind.NewLine)
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Mssql_Top()
        {
            var tokens = new Tokenizer().Tokenize("SELECT TOP 5 [bar] FROM foo", new MsSqlDialect());

            var expected = new Token[]
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Word("TOP"),
                new Whitespace(WhitespaceKind.Space),
                new Number("5"),
                new Whitespace(WhitespaceKind.Space),
                new Word("bar", '['),
                new Whitespace(WhitespaceKind.Space),
                new Word("FROM"),
                new Whitespace(WhitespaceKind.Space),
                new Word("foo")
            };

            Compare(expected, tokens);
        }

        [Fact]
        public void Tokenize_Pg_Regex_Match()
        {
            var tokens = new Tokenizer().Tokenize("SELECT col ~ '^a', col ~* '^a', col !~ '^a', col !~* '^a'", new GenericDialect());

            var expected = new List<Token>
            {
                new Word("SELECT"),
                new Whitespace(WhitespaceKind.Space),
                new Word("col"),
                new Whitespace(WhitespaceKind.Space),
                new Tilde(),
                new Whitespace(WhitespaceKind.Space),
                new SingleQuotedString("^a"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Word("col"),
                new Whitespace(WhitespaceKind.Space),
                new TildeAsterisk(),
                new Whitespace(WhitespaceKind.Space),
                new SingleQuotedString("^a"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Word("col"),
                new Whitespace(WhitespaceKind.Space),
                new ExclamationMarkTilde(),
                new Whitespace(WhitespaceKind.Space),
                new SingleQuotedString("^a"),
                new Comma(),
                new Whitespace(WhitespaceKind.Space),
                new Word("col"),
                new Whitespace(WhitespaceKind.Space),
                new ExclamationMarkTildeAsterisk(),
                new Whitespace(WhitespaceKind.Space),
                new SingleQuotedString("^a")
            };
            
            Assert.Equal(expected, tokens);
        }
    }
}