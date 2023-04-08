namespace SqlParser.Tokens;

public abstract class Token
{
    public Location Location { get; private set; } = new ();

    public void SetLocation(Location location)
    {
        Location = location;
    }
}
