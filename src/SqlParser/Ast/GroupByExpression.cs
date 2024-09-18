namespace SqlParser.Ast;

public abstract record GroupByExpression : IWriteSql, IElement
{
    public record All(Sequence<GroupByWithModifier>? Modifiers = null) : GroupByExpression;

    public record Expressions(Sequence<Expression> ColumnNames, Sequence<GroupByWithModifier>? Modifiers = null) : GroupByExpression;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case All all:
                writer.Write("GROUP BY ALL");
                WriteModifiers(all.Modifiers);
                break;

            case Expressions expressions:
                writer.Write($"GROUP BY {expressions.ColumnNames.ToSqlDelimited()}");
                WriteModifiers(expressions.Modifiers);
                break;
        }

        return;

        void WriteModifiers(Sequence<GroupByWithModifier>? modifiers)
        {
            if (!modifiers.SafeAny()){ return; }

            writer.WriteSql($" ");
            for (var i = 0; i < modifiers!.Count; i++)
            {
                writer.WriteSql($"{modifiers[i]}");
                if (i < modifiers.Count - 1)
                {
                    writer.Write(" ");
                }
            }
        }
    }
}