
namespace SqlParser.Ast;

public record InsertOperation([property: Visit(0)] ObjectName Name, [property: Visit(1)] Statement.Select? Source)
{
    /// Only for Sqlite
    public SqliteOnConflict Or { get; init; }
    /// Only for MySql
    public bool Ignore { get; init; }
    /// INTO - optional keyword
    public bool Into { get; init; }
    /// table_name as foo (for PostgreSQL)
    public Ident? Alias { get; init; }
    /// COLUMNS
    public Sequence<Ident>? Columns { get; init; }
    /// Overwrite (Hive)
    public bool Overwrite { get; init; }
    /// partitioned insert (Hive)
    [Visit(2)] public Sequence<Expression>? Partitioned { get; init; }
    /// Columns defined after PARTITION
    public Sequence<Ident>? AfterColumns { get; init; }
    /// whether the insert has the table keyword (Hive)
    public bool Table { get; init; }
    public OnInsert? On { get; init; }
    /// RETURNING
    [Visit(3)] public Sequence<SelectItem>? Returning { get; init; }
    /// Only for mysql
    public bool ReplaceInto { get; set; }
    /// Only for mysql
    public MySqlInsertPriority Priority { get; init; }
    public InsertAliases? InsertAlias { get; init; }
}
