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
            return Symbols.EndOfFile;
        }
        return _finished ? Symbols.EndOfFile : _characters[_index];
    }

    public char? Next()
    {
        var peeked = Peek();
        _index++;

        if (_index >= _characters.Length)
        {
            _finished = true;
            return peeked;
        }

        if (peeked == Symbols.NewLine)
        {
            _location.NewLine();
        }
        else
        {
            _location.MoveCol();
        }

        return peeked;
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

    internal readonly Location CloneLocation()
    {
        return Location.From(_location.Line, _location.Column);
    }

    internal readonly State Clone()
    {
        return new State(_index, CloneLocation(), _characters);
    }
}