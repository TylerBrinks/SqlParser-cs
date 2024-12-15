namespace SqlParser.Ast;

public record IdentityProperty(IdentityPropertyFormatKind? Parameters, IdentityPropertyOrder? Order);//: IWriteSql, IElement
//{
//    public void ToSql(SqlTextWriter writer)
//    {
//        //writer.WriteSql($"{Parameters}, {Order}");
//    }
//}

public record IdentityParameters(Expression Seed, Expression Increment) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Seed}, {Increment}");
    }
}

public abstract record IdentityPropertyKind : IWriteSql, IElement
{
    public record Autoincrement(IdentityProperty Property) : IdentityPropertyKind;

    public record Identity(IdentityProperty Property) : IdentityPropertyKind;

    public void ToSql(SqlTextWriter writer)
    {
        var (command, property) = this switch
        {
            Autoincrement a => ("AUTOINCREMENT", a.Property),
            Identity i => ("IDENTITY", i.Property),
            _ => throw new InvalidOperationException()
        };

        writer.WriteSql($"{command}");

        if (property.Parameters != null)
        {
            writer.WriteSql($"{property.Parameters}");
        }

        if (property.Order != null)
        {
            writer.WriteSql($"{property.Order}");
        }
    }
}

public abstract record IdentityPropertyFormatKind : IWriteSql, IElement
{
    public record FunctionCall(IdentityParameters Parameters) : IdentityPropertyFormatKind;
    public record StartAndIncrement(IdentityParameters Parameters) : IdentityPropertyFormatKind;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case FunctionCall f:
                writer.WriteSql($"({f.Parameters.Seed}, {f.Parameters.Increment})");
                break;

            case StartAndIncrement s:
                writer.WriteSql($" START {s.Parameters.Seed} INCREMENT {s.Parameters.Increment}");
                break;
        }
    }
}

public enum IdentityPropertyOrder
{
    Order,
    NoOrder
}

public abstract record ColumnPolicy : IWriteSql, IElement
{
    public record MaskingPolicy(ColumnPolicyProperty Property) : ColumnPolicy;
    public record ProjectionPolicy(ColumnPolicyProperty Property) : ColumnPolicy;
    public void ToSql(SqlTextWriter writer)
    {
        var (command, property) = this switch
        {
            MaskingPolicy m => ("MASKING POLICY", m.Property),
            ProjectionPolicy pr => ("PROJECTION POLICY", pr.Property),
            _ => throw new InvalidOperationException()
        };

        if (property.With)
        {
            writer.Write("WITH ");
        }

        writer.WriteSql($"{command} {property.PolicyName}");

        if (property.UsingColumns.SafeAny())
        {
            writer.WriteSql($" USING ({property.UsingColumns.ToSqlDelimited()})");
        }
    }
}

public record ColumnPolicyProperty(bool With, Ident PolicyName, Sequence<Ident>? UsingColumns);

public record TagsColumnOption(bool With, Sequence<Tag> Tags) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        if (With)
        {
            writer.WriteSql($"WITH ");
        }
        writer.WriteSql($"TAG ({Tags.ToSqlDelimited()})");
    }
}