namespace SqlParser.Ast;

public record ProcedureParam(Ident Name, DataType DataType) : IWriteSql, IElement
{
    /// <summary>
    /// Default value for the parameter
    /// </summary>
    public Expression? Default { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name} {DataType}");

        if (Default != null)
        {
            writer.WriteSql($" DEFAULT {Default}");
        }
    }
}
