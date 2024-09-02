namespace SqlParser.Ast;

public record UnionField(Ident FieldName, DataType FieldType) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{FieldName} {FieldType}");
    }
}
