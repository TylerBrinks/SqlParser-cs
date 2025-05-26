﻿using System.Reflection.Metadata.Ecma335;

namespace SqlParser.Ast;

/// <summary>
/// Table object
/// </summary>
/// <param name="Name">Table name</param>
/// <param name="SchemaName">Schema name</param>
public record Table(string Name, string? SchemaName = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        if (SchemaName != null)
        {
            writer.WriteSql($"TABLE {SchemaName}.{Name}");
        }
        else
        {
            writer.Write($"TABLE {Name}");
        }
    }
}

/// <summary>
/// Table alias
/// </summary>
/// <param name="Name">Name identifier</param>
public record TableAlias(Ident Name, bool AsKeyword = true, Sequence<Ident>? Columns = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        Name.ToSql(writer);

        if (Columns.SafeAny())
        {
            writer.WriteSql($" ({Columns})");
        }
    }
}