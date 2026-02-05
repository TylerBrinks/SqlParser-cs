namespace SqlParser.Ast;

/// <summary>
/// PostgreSQL partition bound specification
/// </summary>
public abstract record PartitionBound : IWriteSql, IElement
{
    /// <summary>
    /// FOR VALUES IN (val1, val2, ...)
    /// </summary>
    public record In(Sequence<Expression> Values) : PartitionBound
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"FOR VALUES IN ({Values.ToSqlDelimited()})");
        }
    }

    /// <summary>
    /// FOR VALUES FROM (val1, ...) TO (val2, ...)
    /// </summary>
    public record FromTo(Sequence<PartitionBoundValue> From, Sequence<PartitionBoundValue> To) : PartitionBound
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"FOR VALUES FROM ({From.ToSqlDelimited()}) TO ({To.ToSqlDelimited()})");
        }
    }

    /// <summary>
    /// FOR VALUES WITH (MODULUS n, REMAINDER m)
    /// </summary>
    public record Hash(int Modulus, int Remainder) : PartitionBound
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"FOR VALUES WITH (MODULUS {Modulus}, REMAINDER {Remainder})");
        }
    }

    /// <summary>
    /// DEFAULT
    /// </summary>
    public record Default : PartitionBound
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DEFAULT");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Partition bound value for FROM/TO clauses
/// </summary>
public abstract record PartitionBoundValue : IWriteSql, IElement
{
    /// <summary>
    /// An expression value
    /// </summary>
    public record Expression(Ast.Expression Expr) : PartitionBoundValue
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expr}");
        }
    }

    /// <summary>
    /// MINVALUE
    /// </summary>
    public record MinValue : PartitionBoundValue
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("MINVALUE");
        }
    }

    /// <summary>
    /// MAXVALUE
    /// </summary>
    public record MaxValue : PartitionBoundValue
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("MAXVALUE");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
