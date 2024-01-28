//namespace SqlParser.Ast;

///// <summary>
///// Wildcard expressions
///// </summary>
//public abstract record WildcardExpression : Expression
//{
//    /// <summary>
//    /// Expression
//    /// </summary>
//    /// <param name="Expression">Expression</param>
//    public record Expr(Expression Expression) : WildcardExpression;
//    /// <summary>
//    /// Qualified expression
//    /// </summary>
//    /// <param name="Name">Object name</param>
//    public record QualifiedWildcard(ObjectName Name) : WildcardExpression;
//    /// <summary>
//    /// Wildcard expression
//    /// </summary>
//    public record Wildcard : WildcardExpression;

//    public static implicit operator FunctionArgExpression(WildcardExpression expr)
//    {
//        return expr switch
//        {
//            Expr e => new FunctionArgExpression.FunctionExpression(e.Expression),
//            QualifiedWildcard q => new FunctionArgExpression.QualifiedWildcard(q.Name),
//            _ => new FunctionArgExpression.Wildcard()
//        };
//    }
//}