namespace SqlParser.Ast;

public record CreateIndex([property: Visit(0)] ObjectName? Name, [property: Visit(1)] ObjectName TableName) : IWriteSql, IIfNotExists
{
    /// <summary>
    /// Index type (USING clause)
    /// </summary>
    public IndexType? IndexType { get; init; }
    /// <summary>
    /// Custom index type name when IndexType is Custom
    /// </summary>
    public Ident? CustomIndexTypeName { get; init; }
    /// <summary>
    /// Columns for the index
    /// </summary>
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

        if (IndexType != null && IndexType != Ast.IndexType.None)
        {
            if (IndexType == Ast.IndexType.Custom && CustomIndexTypeName != null)
            {
                writer.WriteSql($" USING {CustomIndexTypeName}");
            }
            else
            {
                var indexTypeName = IndexType switch
                {
                    Ast.IndexType.BTree => "BTREE",
                    Ast.IndexType.Hash => "HASH",
                    Ast.IndexType.GIN => "GIN",
                    Ast.IndexType.GiST => "GIST",
                    Ast.IndexType.SPGiST => "SPGIST",
                    Ast.IndexType.BRIN => "BRIN",
                    Ast.IndexType.Bloom => "BLOOM",
                    _ => IndexType.ToString()!.ToUpperInvariant()
                };
                writer.Write($" USING {indexTypeName}");
            }
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