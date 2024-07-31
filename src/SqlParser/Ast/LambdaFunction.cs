namespace SqlParser.Ast;

public record LambdaFunction(OneOrManyWithParens<Ident> Params, Expression Body) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Params} -> {Body}");
    }
}

public abstract record OneOrManyWithParens<T> : IWriteSql where T : IWriteSql
{
    public record One(T Value) : OneOrManyWithParens<T>;
    public record Many(Sequence<T> Values) : OneOrManyWithParens<T>;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case One o:
                writer.WriteSql($"{o.Value}");
                break;

            case Many m:
                writer.WriteSql($"({m.Values.ToSqlDelimited()})");
                break;
        }
    }
}
