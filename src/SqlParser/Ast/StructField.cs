namespace SqlParser.Ast;

public record StructField(DataType FieldType, Ident? FieldName = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        if (FieldName != null)
        {
            writer.WriteSql($"{FieldName} {FieldType}");
        }
        else
        {
            writer.WriteSql($"{FieldType}");
        }
    }
}