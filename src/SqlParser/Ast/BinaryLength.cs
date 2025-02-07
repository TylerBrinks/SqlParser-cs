namespace SqlParser.Ast;


public abstract record BinaryLength : IWriteSql, IElement
{
    /// <param name="Length">Default (if VARYING) or maximum (if not VARYING) length</param>
    public record IntegerLength(ulong Length) : BinaryLength;

    public record Max : BinaryLength;

    public void ToSql(SqlTextWriter writer)
    {
        if (this is IntegerLength i)
        {
            writer.WriteSql($"{i.Length}");
        }
        else
        {
            writer.Write("MAX");
        }
    }
}
