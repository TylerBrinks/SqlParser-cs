namespace SqlParser.Ast;

/// <summary>
/// Sql option
/// </summary>
public abstract record SqlOption : IWriteSql, IElement
{
    public record Clustered(TableOptionsClustered Option) : SqlOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Option}");
        }
    }

    public record Identifier(Ident Name) : SqlOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name}");
        }
    }

    public record KeyValue(Ident Name, Expression Value) : SqlOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name} = {Value}");
        }
    }

    public record Partition(Ident ColumnName, Sequence<Expression> ForValues, PartitionRangeDirection? RangeDirection = null) : SqlOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            var direction = RangeDirection switch
            {
                PartitionRangeDirection.Left => " LEFT",
                PartitionRangeDirection.Right => " RIGHT",
                _ => string.Empty
            };

            writer.WriteSql($"PARTITION ({ColumnName} RANGE{direction} FOR VALUES ({ForValues.ToSqlDelimited()}))");
        }
    }


    public abstract void ToSql(SqlTextWriter writer);
}