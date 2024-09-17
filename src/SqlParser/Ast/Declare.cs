namespace SqlParser.Ast;

/// <summary>
/// Represents an expression assignment within a variable `DECLARE` statement.
///
/// DECLARE variable_name := 42
/// DECLARE variable_name DEFAULT 42
/// </summary>
public abstract record DeclareAssignment(Expression Expression) : IWriteSql, IElement
{
    public record DeclareExpression(Expression Declaration) : DeclareAssignment(Declaration);
    public record Default(Expression DefaultExpression) : DeclareAssignment(DefaultExpression);
    public record Assignment(Expression AssignmentExpression) : DeclareAssignment(AssignmentExpression);
    public record For(Expression ForExpression) : DeclareAssignment(ForExpression);
    public record MsSqlAssignment(Expression Expression) : DeclareAssignment(Expression);

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case DeclareExpression:
                writer.WriteSql($"{Expression}");
                break;

            case Default:
                writer.WriteSql($"DEFAULT {Expression}");
                break;

            case Assignment:
                writer.WriteSql($":= {Expression}");
                break;

            case For:
                writer.WriteSql($"FOR {Expression}");
                break;

            case MsSqlAssignment:
                writer.WriteSql($"= {Expression}");
                break;
        }
    }
}


public record Declare(Sequence<Ident> Names, DataType? DataType, DeclareAssignment? Assignment, DeclareType? DeclareType) : IWriteSql, IElement
{
    /// <summary>
    /// Causes the cursor to return data in binary rather than in text format.
    /// </summary>
    public bool? Binary { get; init; }
    /// <summary>
    /// None = Not specified
    /// Some(true) = INSENSITIVE
    /// Some(false) = ASENSITIVE
    /// </summary>
    public bool? Sensitive { get; init; }
    /// <summary>
    /// None = Not specified
    /// Some(true) = SCROLL
    /// Some(false) = NO SCROLL
    /// </summary>
    public bool? Scroll { get; init; }
    /// <summary>
    /// None = Not specified
    /// Some(true) = WITH HOLD, specifies that the cursor can continue to be used after the transaction that created it successfully commits
    /// Some(false) = WITHOUT HOLD, specifies that the cursor cannot be used outside of the transaction that created it
    /// </summary>
    public bool? Hold { get; init; }
    /// <summary>
    /// Select
    /// </summary>
    //public Select? Query { get; init; }
    public Query? ForQuery { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteDelimited(Names, Constants.SpacedComma);
        if (Binary.HasValue && Binary.Value)
        {
            writer.Write(" BINARY");
        }

        if (Sensitive.HasValue)
        {
            writer.Write(Sensitive.Value ? " INSENSITIVE" : " ASENSITIVE");
        }

        if (Scroll.HasValue)
        {
            writer.Write(Scroll.Value ?  " SCROLL" : " NO SCROLL");
        }

        if (DeclareType != null)
        {
            writer.WriteSql($" {DeclareType}");
        }

        if (Hold.HasValue)
        {
            writer.Write(Hold.Value ? " WITH HOLD" : " WITHOUT HOLD");
        }

        if (ForQuery != null)
        {
            writer.WriteSql($" FOR {ForQuery}");
        }
        
        if (DataType != null)
        {
            writer.WriteSql($" {DataType}");
        }

        if (Assignment != null)
        {
            writer.WriteSql($" {Assignment}");
        }
    }
}

public enum DeclareType
{
    Cursor,
    ResultSet,
    Exception
}