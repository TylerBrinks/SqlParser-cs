namespace SqlParser.Ast;

/// <summary>
/// PostgreSQL function security specifier: SECURITY DEFINER or SECURITY INVOKER
/// </summary>
public enum FunctionSecurity
{
    Definer,
    Invoker
}

/// <summary>
/// Value for a SET configuration parameter in a CREATE FUNCTION statement
/// </summary>
public abstract record FunctionSetValue : IWriteSql, IElement
{
    /// <summary>
    /// SET param = value1, value2, ...
    /// </summary>
    public record Values(Sequence<Expression> Exprs) : FunctionSetValue
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Exprs.ToSqlDelimited()}");
        }
    }

    /// <summary>
    /// SET param FROM CURRENT
    /// </summary>
    public record FromCurrent : FunctionSetValue
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FROM CURRENT");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// A SET configuration_parameter clause in a CREATE FUNCTION statement
/// PostgreSQL: SET param = value or SET param FROM CURRENT
/// </summary>
public record FunctionDefinitionSetParam(Ident Name, FunctionSetValue Value) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"SET {Name} ");
        switch (Value)
        {
            case FunctionSetValue.Values v:
                writer.Write("= ");
                v.ToSql(writer);
                break;
            case FunctionSetValue.FromCurrent:
                writer.Write("FROM CURRENT");
                break;
        }
    }
}
