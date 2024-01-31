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
            case TableFactor t:
            {
                visitor.PreVisitTableFactor(t);

                if (t is TableFactor.Table table)
                {
                    visitor.PreVisitRelation(table.Name);
                    VisitChildren(this, visitor);
                    visitor.PostVisitRelation(table.Name);
                }

                visitor.PostVisitTableFactor(t);
                return ControlFlow.Continue;
            }

            case Expression e:
                {
                    var flow = visitor.PreVisitExpression(e);

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

            var child = (IElement)value;
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

        // No decorated properties uses the default visit order.
        // No need to look for additional properties
        if (!decorated.Any())
        {
            return properties.ToArray();
        }

        // Visit orders are not specified in the constructor; return the decorated list.
        if (decorated.Count == properties.Length)
        {
            return decorated.ToArray();
        }

        // Although identified as properties, primary constructor parameters 
        // use parameter attributes, not property attributes and must be identified
        // apart from the property list. This find their order and inserts
        // the missing properties into the decorated property list.
        try
        {
            var constructors = elementType.GetConstructors();
            var primaryConstructor = constructors.Single();
            var constructorParams = primaryConstructor.GetParameters();

            var decoratedParameters = constructorParams.Where(p => p.GetCustomAttribute<VisitAttribute>() != null)
                .OrderBy(p => p.GetCustomAttribute<VisitAttribute>()!.Order)
                .Select(p => (Property:p, p.GetCustomAttribute<VisitAttribute>()!.Order))
                .ToList();

            foreach (var param in decoratedParameters)
            {
                var property = properties.FirstOrDefault(p => p.Name == param.Property.Name);

                if (property != null)
                {
                    decorated.Insert(param.Order, property);
                }
            }
        }
        // ReSharper disable once EmptyGeneralCatchClause
        catch { }

        return decorated.ToArray();
    }
}

public abstract class Visitor
{
    public virtual ControlFlow PreVisitQuery(Query query)
    {
        return ControlFlow.Continue;
    }
   
    public virtual ControlFlow PostVisitQuery(Query query)
    {
        return ControlFlow.Continue;
    }

    public virtual ControlFlow PreVisitTableFactor(TableFactor tableFactor)
    {
        return ControlFlow.Continue;
    }

    public virtual ControlFlow PostVisitTableFactor(TableFactor tableFactor)
    {
        return ControlFlow.Continue;
    }

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