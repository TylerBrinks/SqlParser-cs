namespace SqlParser.Ast;

/// <summary>
/// Function argument
/// </summary>
public abstract record FunctionArg : IWriteSql, IElement
{
    /// <summary>
    /// Named function argument
    /// </summary>
    /// <param name="Name">Name identifier</param>
    /// <param name="Arg">Function argument expression</param>
    public record Named(Ident Name, FunctionArgExpression Arg, FunctionArgOperator Operator) : FunctionArg
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name} {Operator} {Arg}");
        }
    }
    /// <summary>
    /// Unnamed function argument
    /// </summary>
    /// <param name="FunctionArgExpression">Function argument expression</param>
    public record Unnamed(FunctionArgExpression FunctionArgExpression) : FunctionArg
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FunctionArgExpression.ToSql(writer);
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Function argument expression
/// </summary>
public abstract record FunctionArgExpression : IWriteSql, IElement
{
    /// <summary>
    /// Function expression
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record FunctionExpression(Expression Expression) : FunctionArgExpression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Expression.ToSql(writer);
        }
    }
    /// <summary>
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    /// </summary>
    public record QualifiedWildcard(ObjectName Name) : FunctionArgExpression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"{Name}.*");
        }
    }
    /// <summary>
    /// An unqualified `*`
    /// </summary>
    public record Wildcard : FunctionArgExpression
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("*");
        }
    }
    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Function definition
/// </summary>
public abstract record FunctionDefinition : IWriteSql
{
    /// <summary>
    /// Single quoted definition
    /// </summary>
    /// <param name="Value">String value</param>
    public record SingleQuotedDef(string Value) : FunctionDefinition;
    /// <summary>
    /// Double quoted definition
    /// </summary>
    /// <param name="Value">String value</param>
    public record DoubleDollarDef(string Value) : FunctionDefinition;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case SingleQuotedDef s:
                writer.WriteSql($"'{s.Value}'");
                break;

            case DoubleDollarDef d:
                writer.WriteSql($"$${d.Value}$$");
                break;
        }
    }
}

public abstract record FunctionArgOperator : IWriteSql, IElement
{
    public record Equal : FunctionArgOperator
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("=");
        }
    }

    public record RightArrow : FunctionArgOperator
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("=>");
        }
    }

    public record Assignment : FunctionArgOperator
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(":=");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Create function body
/// </summary>
public record CreateFunctionBody : IWriteSql, IElement
{
    // LANGUAGE lang_name
    public Ident? Language { get; internal set; }
    // IMMUTABLE | STABLE | VOLATILE
    public FunctionBehavior? Behavior { get; internal set; }
    // AS 'definition'
    // Note that Hive's `AS class_name` is also parsed here.
    public FunctionDefinition? As { get; internal set; }
    // RETURN expression
    public Expression? Return { get; internal set; }
    // USING ... (Hive only)
    public CreateFunctionUsing? Using { get; internal set; }

    public void ToSql(SqlTextWriter writer)
    {
        if (Language != null)
        {
            writer.WriteSql($" LANGUAGE {Language}");
        }

        if (Behavior != null)
        {
            writer.WriteSql($" {Behavior}");
        }

        if (As != null)
        {
            writer.WriteSql($" AS {As}");
        }

        if (Return != null)
        {
            writer.WriteSql($" RETURN {Return}");
        }

        if (Using != null)
        {
            writer.WriteSql($" {Using}");
        }
    }
}

/// <summary>
/// Create function using
/// </summary>
public abstract record CreateFunctionUsing : IWriteSql
{
    public abstract record CreateFunctionUsingValue(string Value) : CreateFunctionUsing;
    /// <summary>
    /// None
    /// </summary>
    public record None : CreateFunctionUsing;
    /// <summary>
    /// Create using Jar
    /// </summary>
    /// <param name="Value">String value</param>
    public record Jar(string Value) : CreateFunctionUsingValue(Value);
    /// <summary>
    /// Create using file
    /// </summary>
    /// <param name="Value">String value</param>
    public record File(string Value) : CreateFunctionUsingValue(Value);
    /// <summary>
    /// Create using archive
    /// </summary>
    /// <param name="Value">String value</param>
    public record Archive(string Value) : CreateFunctionUsingValue(Value);

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("USING ");

        switch (this)
        {
            case Jar j:
                writer.Write($"JAR '{j.Value}'");
                break;

            case File f:
                writer.Write($"FILE '{f.Value}'");
                break;

            case Archive a:
                writer.Write($"ARCHIVE '{a.Value}'");
                break;
        }
    }
}
