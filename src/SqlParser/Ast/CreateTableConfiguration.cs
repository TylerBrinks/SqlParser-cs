
namespace SqlParser.Ast;

public record CreateTableConfiguration(
    Expression? PartitionBy, WrappedCollection<Ident>? ClusterBy, Sequence<SqlOption>? Options) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        throw new NotImplementedException();
    }
}