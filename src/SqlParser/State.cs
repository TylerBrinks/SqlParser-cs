namespace SqlParser;

public ref struct State
{
    private readonly ReadOnlySpan<char> _characters;
    private readonly Location _location;
    private int _index;
    private bool _finished;

    internal State(ReadOnlySpan<char> sql)
    {
        _characters = sql;
        _location = new Location();
    }

    private State(int index, Location location, ReadOnlySpan<char> characters)
    {
        _index = index;
        _location = location;
        _characters = characters;
    }

    public char Peek()
    {
        if (_finished)
        {
            return Symbols.EndOfFile;
        }

        if (_index >= _characters.Length)
        {
            throw new ParserException($"Parser unable to read character at index {_index}");
        }
        return _finished ? Symbols.EndOfFile : _characters[_index];
    }

    public void Next()
    {
        _index++;

        if (_index >= _characters.Length)
        {
            _finished = true;
            return;
        }

        if (Peek() == Symbols.NewLine)
        {
            _location.NewLine();
        }
        else
        {
            _location.MoveCol();
        }
    }

    public char? SkipWhile(Func<char, bool> skipPredicate)
    {
        try
        {
            if (skipPredicate(Peek()))
            {
                Next();
            }

            return Peek();
        }
        catch
        {
            return null;
        }
    }

    internal char[] PeekTakeWhile(Func<char, bool> predicate)
    {
        char current;
        var chars = new List<char>();

        while ((current = Peek()) != Symbols.EndOfFile)
        {
            if (predicate(current))
            {
                Next();
                chars.Add(current);
            }
            else
            {
                break;
            }
        }

        return [.. chars];
    }

    internal Location CloneLocation()
    {
        return Location.From(_location.Line, _location.Column);
    }

    internal State Clone()
    {
        return new State(_index, CloneLocation(), _characters);
    }
}