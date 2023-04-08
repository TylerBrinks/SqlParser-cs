namespace SqlParser.Ast;

/// <summary>
/// One item of the comma-separated list following SELECT
/// </summary>
public abstract record SelectItem : IWriteSql, IElement
{
    /// <summary>
    ///  Any expression, not followed by [ AS ] alias
    /// </summary>
    /// <param name="Expression">Select expression</param>
    public record UnnamedExpression(Expression Expression) : SelectItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Expression.ToSql(writer);
        }
    }
    /// <summary>
    /// alias.* or even schema.table.*
    /// </summary>
    /// <param name="Name">Object name</param>
    /// <param name="WildcardAdditionalOptions">Select options</param>
    public record QualifiedWildcard(ObjectName Name, WildcardAdditionalOptions WildcardAdditionalOptions) : SelectItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name}.*{WildcardAdditionalOptions}");
        }
    }
    /// <summary>
    /// An expression, followed by [ AS ] alias
    /// </summary>
    /// <param name="Expression">Select expression</param>
    /// <param name="Alias">Select alias</param>
    public record ExpressionWithAlias(Expression Expression, Ident Alias) : SelectItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Expression} AS {Alias}");
        }
    }
    /// <summary>
    /// An unqualified *
    /// </summary>
    /// <param name="WildcardAdditionalOptions"></param>
    public record Wildcard(WildcardAdditionalOptions WildcardAdditionalOptions) : SelectItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"*{WildcardAdditionalOptions}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);

    public T As<T>() where T : SelectItem
    {
        return (T) this;
    }

    public UnnamedExpression AsUnnamed()
    {
        return As<UnnamedExpression>();
    }

    public Wildcard AsWildcard()
    {
        return As<Wildcard>();
    }
}

/// <summary>
/// Excluded select item
/// </summary>
public abstract record ExcludeSelectItem : IWriteSql, IElement
{
    /// <summary>
    /// Single exclusion
    /// </summary>
    /// <param name="Name">Name identifier</param>
    public record Single(Ident Name) : ExcludeSelectItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"EXCLUDE {Name}");
        }
    }
    /// <summary>
    /// Multiple exclusions
    /// </summary>
    /// <param name="Columns">Name identifiers</param>
    public record Multiple(Sequence<Ident> Columns) : ExcludeSelectItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"EXCLUDE ({Columns})");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Rename select item
/// </summary>
public abstract record RenameSelectItem : IWriteSql, IElement
{
    /// <summary>
    /// Single rename
    /// </summary>
    /// <param name="Name">Name identifier</param>
    public record Single(IdentWithAlias Name) : RenameSelectItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME {Name}");
        }
    }
    /// <summary>
    /// Multiple exclusions
    /// </summary>
    /// <param name="Columns">Name identifiers</param>
    public record Multiple(Sequence<IdentWithAlias> Columns) : RenameSelectItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME ({Columns})");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Expected select item
/// </summary>
/// <param name="FirstElement">First item in the list</param>
/// <param name="AdditionalElements">Additional items</param>
public record ExceptSelectItem(Ident FirstElement, Sequence<Ident> AdditionalElements) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("EXCEPT ");

        if (AdditionalElements.SafeAny())
        {
            writer.WriteSql($"({FirstElement}, {AdditionalElements})");
        }
        else
        {
            writer.WriteSql($"({FirstElement})");
        }
    }
}
