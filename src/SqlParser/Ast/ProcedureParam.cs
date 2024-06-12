namespace SqlParser.Ast;

public record ProcedureParam(Ident Name, DataType DataType) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name} {DataType}");
    }
}
