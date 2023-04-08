namespace SqlParser;

public interface IWriteSql
{
    void ToSql(SqlTextWriter writer);
}