namespace SqlParser.Ast;

/// <summary>
/// Framed boundary
/// </summary>
/// <param name="Units">Window unit flag</param>
/// <param name="StartBound">Boundary start</param>
/// <param name="EndBound">Boundary end</param>
public record WindowFrame(WindowFrameUnit Units, WindowFrameBound? StartBound, WindowFrameBound? EndBound) : IElement;

public abstract record WindowFrameBound : IWriteSql, IElement
{
    public record CurrentRow : WindowFrameBound
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("CURRENT ROW");
        }
    }
    public record Preceding(Expression? Expression) : WindowFrameBound
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Expression == null)
            {
                writer.Write("UNBOUNDED PRECEDING");
            }
            else
            {
                writer.WriteSql($"{Expression} PRECEDING");
            }
        }
    }
    public record Following(Expression? Expression) : WindowFrameBound
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Expression == null)
            {
                writer.Write("UNBOUNDED FOLLOWING");
            }
            else
            {
                writer.WriteSql($"{Expression} FOLLOWING");
            }
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

public record WindowSpec(
    Sequence<Expression>? PartitionBy = null, 
    Sequence<OrderByExpression>? OrderBy = null, 
    WindowFrame? WindowFrame = null,
    Ident? WindowName = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        var delimiter = string.Empty;

        if (WindowName != null)
        {
            delimiter = " ";
            writer.WriteSql($"{WindowName}");
        }

        if (PartitionBy.SafeAny())
        {
            writer.Write(delimiter);
            delimiter = " ";
            writer.WriteSql($"PARTITION BY {PartitionBy}");
        }

        if (OrderBy != null)
        {
            writer.Write(delimiter);
            delimiter = " ";
            writer.WriteSql($"ORDER BY {OrderBy}");
        }

        if (WindowFrame != null)
        {
            writer.Write(delimiter);
            if (WindowFrame.EndBound != null)
            {
                writer.WriteSql($"{WindowFrame.Units} BETWEEN {WindowFrame.StartBound} AND {WindowFrame.EndBound}");
            }
            else
            {
                writer.WriteSql($"{WindowFrame.Units} {WindowFrame.StartBound}");
            }
        }
    }
}

public record NamedWindowDefinition(Ident Name, NamedWindowExpression WindowSpec) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name} AS {WindowSpec}");
    }
}

public abstract record NamedWindowExpression : IWriteSql, IElement
{
    public record NamedWindow(Ident Expression) : NamedWindowExpression{}
    public record NamedWindowSpec(WindowSpec Spec) : NamedWindowExpression { }

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case NamedWindow w:
                writer.WriteSql($"{w.Expression}");
                break;

            case NamedWindowSpec s:
                writer.WriteSql($"({s.Spec})");
                break;
        }
    }
}