
namespace SqlParser;

public interface IState
{
    void Next();
    char? SkipWhile(Func<char, bool> skipPredicate);
}

public class State : IState
{
    private readonly char[] _characters;
    private readonly Location _location;
    private long _index;
    private bool _finished;

    internal State(string sql)
    {
        _characters = sql.ToCharArray();
        _location = new Location();
    }

    private State(long index, Location location, char[] characters)
    {
        _index= index;
        _location = location;
        _characters = characters;
    }

    internal char Peek()
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

    internal Location CloneLocation()
    {
        return Location.From(_location.Line, _location.Column);
    }

    internal State Clone()
    {
        var cloneCharacters = new char[_characters.Length];
        _characters.CopyTo(cloneCharacters, 0);
        return new State(_index, CloneLocation(), cloneCharacters);
    }
}