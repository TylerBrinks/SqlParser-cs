namespace SqlParser.Ast;

/// <summary>
/// SQL column definition
/// </summary>
/// <param name="Name">Column name</param>
/// <param name="DataType">Column data type</param>
/// <param name="Collation">Collation</param>
/// <param name="Options">Column options</param>
public record ColumnDef(Ident Name, DataType DataType, ObjectName? Collation = null, Sequence<ColumnOptionDef>? Options = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        if (DataType is DataType.Unspecified)
        {
            writer.WriteSql($"{Name}");
        }
        else
        {
            writer.WriteSql($"{Name} {DataType}");
        }

        if (Collation != null)
        {
            writer.WriteSql($" COLLATE {Collation}");
        }

        if (Options != null)
        {
            foreach (var option in Options)
            {
                writer.WriteSql($" {option}");
            }
        }
    }
}

/// <summary>
/// An optionally-named ColumnOption: [ CONSTRAINT name ] column-option.
///
/// Note that implementations are substantially more permissive than the ANSI
/// specification on what order column options can be presented in, and whether
/// they are allowed to be named. The specification distinguishes between
/// constraints (NOT NULL, UNIQUE, PRIMARY KEY, and CHECK), which can be named
/// and can appear in any order, and other options (DEFAULT, GENERATED), which
/// cannot be named and must appear in a fixed order. `PostgreSQL`, however,
/// allows preceding any option with `CONSTRAINT name`, even those that are
/// not really constraints, like NULL and DEFAULT. MSSQL is less permissive,
/// allowing DEFAULT, UNIQUE, PRIMARY KEY and CHECK to be named, but not NULL or
/// NOT NULL constraints (the last of which is in violation of the spec).
///
/// For maximum flexibility, we don't distinguish between constraint and
/// non-constraint options, lumping them all together under the umbrella of
/// "column options," and we allow any column option to be named.
/// </summary>
/// <param name="Name">Name identifier</param>
/// <param name="Option">Column Options</param>
public record ColumnOptionDef(ColumnOption Option, Ident? Name = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        if (Name != null) 
        {
            writer.Write($"CONSTRAINT {Name} ");
        }
        writer.WriteSql($"{Option}");
    }
}
