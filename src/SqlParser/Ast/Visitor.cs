using System.Reflection;

namespace SqlParser.Ast;

public enum ControlFlow
{
    Continue,
    Break
}

public interface IElement
{
    public ControlFlow Visit(Visitor visitor)
    {
        switch (this)
        {
            case ObjectName o:
            {
                var flow = visitor.PreVisitRelation(o);
                if (flow == ControlFlow.Break)
                {
                    return flow;
                }
                VisitChildren(this, visitor);
                return visitor.PostVisitRelation(o);
            }

            case Expression e:
            {
                var flow =visitor.PreVisitExpression(e);

                if (flow == ControlFlow.Break)
                {
                    return flow;
                }
                VisitChildren(this, visitor);
                return visitor.PostVisitExpression(e);
            }

            case Statement s:
            {
                var flow = visitor.PreVisitStatement(s);
                if (flow == ControlFlow.Break)
                {
                    return flow;
                }
            
                VisitChildren(this, visitor);
                return visitor.PostVisitStatement(s);
            }

            default:
                VisitChildren(this, visitor);
                return ControlFlow.Continue;
        }
    }

    private static void VisitChildren(IElement element, Visitor visitor)
    {
        var properties = GetVisitableChildProperties(element);

        foreach (var property in properties)
        {
            if (!property.PropertyType.IsAssignableTo(typeof(IElement)))
            {
                continue;
            }

            var value = property.GetValue(element);

            if (value == null)
            {
                continue;
            }

            var child = (IElement) value;
            child.Visit(visitor);
        }
    }

    internal static PropertyInfo[] GetVisitableChildProperties(IElement element)
    {
        var elementType = element.GetType();

        // Public and not static
        var properties = elementType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var decorated = properties.Where(p => p.GetCustomAttribute<VisitAttribute>() != null)
            .OrderBy(p => p.GetCustomAttribute<VisitAttribute>()!.Order)
            .ToList();

        if (decorated.Any())
        {
            properties = decorated.ToArray();
        }

        return properties.ToArray();
    }
}

public abstract class Visitor
{
    public virtual ControlFlow PreVisitRelation(ObjectName relation)
    {
        return ControlFlow.Continue;
    }

    public virtual ControlFlow PostVisitRelation(ObjectName relation)
    {
        return ControlFlow.Continue;
    }

    public virtual ControlFlow PreVisitExpression(Expression expression)
    {
        return ControlFlow.Continue;
    }

    public virtual ControlFlow PostVisitExpression(Expression expression)
    {
        return ControlFlow.Continue;
    }

    public virtual ControlFlow PreVisitStatement(Statement statement)
    {
        return ControlFlow.Continue;
    }

    public virtual ControlFlow PostVisitStatement(Statement statement)
    {
        return ControlFlow.Continue;
    }
}