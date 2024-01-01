namespace SqlParser.Ast;

public record ProcedureParam(Ident Name, DataType DataType) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name} {DataType}");
    }
}
