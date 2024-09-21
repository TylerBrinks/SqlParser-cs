namespace SqlParser.Ast;

public record CreateIndex([property: Visit(0)] ObjectName? Name, [property: Visit(1)] ObjectName TableName) : IWriteSql, IIfNotExists
{
    public Ident? Using { get; init; }
    [Visit(2)] public Sequence<OrderByExpression>? Columns { get; init; }
    public bool Unique { get; init; }
    public bool IfNotExists { get; init; }
    public bool Concurrently { get; init; }
    public Sequence<Ident>? Include { get; init; }
    public bool? NullsDistinct { get; init; }
    public Sequence<Expression>? With { get; init; }
    public Expression? Predicate { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        var unique = Unique ? "UNIQUE " : null;
        var ifNot = IfNotExists ? $"{((IIfNotExists)this).IfNotExistsText} " : null;
        var concurrently = Concurrently ? "CONCURRENTLY " : null;

        writer.WriteSql($"CREATE {unique}INDEX {concurrently}{ifNot}");

        if (Name != null)
        {
            writer.WriteSql($"{Name} ");
        }

        writer.WriteSql($"ON {TableName}");

        if (Using != null)
        {
            writer.WriteSql($" USING {Using}");
        }

        writer.Write($"({Columns.ToSqlDelimited(Symbols.Comma)})");

        if (Include.SafeAny())
        {
            writer.Write($" INCLUDE ({Include.ToSqlDelimited(Symbols.Comma)})");
        }

        if (NullsDistinct.HasValue)
        {
            writer.Write(NullsDistinct.Value ? " NULLS DISTINCT" : " NULLS NOT DISTINCT");
        }

        if (With.SafeAny())
        {
            writer.WriteSql($" WITH ({With.ToSqlDelimited()})");
        }

        if (Predicate != null)
        {
            writer.WriteSql($" WHERE {Predicate}");
        }
    }
}