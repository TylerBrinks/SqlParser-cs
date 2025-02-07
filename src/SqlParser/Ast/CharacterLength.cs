namespace SqlParser.Ast;


public abstract record CharacterLength : IWriteSql, IElement
{
    /// <summary>
    /// Information about [character length][1], including length and possibly unit.
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-length"/> 
    /// </summary>
    /// <param name="Length">Default (if VARYING) or maximum (if not VARYING) length</param>
    /// <param name="Unit">Optional unit. If not informed, the ANSI handles it as CHARACTERS implicitly</param>
    public record IntegerLength(ulong Length, CharLengthUnit Unit = CharLengthUnit.None) : CharacterLength;

    public record Max : CharacterLength;

    public void ToSql(SqlTextWriter writer)
    {
        if (this is IntegerLength i)
        {
            writer.WriteSql($"{i.Length}");
            if (i.Unit != CharLengthUnit.None)
            {
                writer.Write($" {i.Unit}");
            }
        }
        else
        {
            writer.Write("MAX");
        }
    }
}