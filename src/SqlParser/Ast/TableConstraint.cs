namespace SqlParser.Ast;

/// <summary>
/// A table-level constraint, specified in a CREATE TABLE or an
/// ALTER TABLE ADD constraint statement.
/// </summary>
public abstract record TableConstraint : IWriteSql, IElement
{
    /// <summary>
    /// Unique table constraint
    /// <example>
    /// <c>
    /// [ CONSTRAINT name ] { PRIMARY KEY | UNIQUE } (columns)
    /// </c>
    /// </example>
    /// </summary>
    public record Unique(Sequence<Ident> Columns) : TableConstraint
    {
        public Ident? Name { get; init; }
        public bool IsPrimaryKey { get; init; }
        public ConstraintCharacteristics? Characteristics { get; init; }
        public Keyword? Conflict { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var primary = IsPrimaryKey ? "PRIMARY KEY" : "UNIQUE";
            writer.WriteConstraint(Name);
            writer.WriteSql($"{primary} ({Columns})");
            if (Conflict != null) {
                writer.WriteSql($" ON CONFLICT {Conflict}");
            }

        }
    }
    /// <summary>
    /// A referential integrity constraint
    /// <example>
    /// <c>
    /// REFERENCES foreign_table (referred_columns)
    /// { [ON DELETE referential_action] [ON UPDATE referential_action] |
    ///   [ON UPDATE referential_action] [ON DELETE referential_action]
    /// }`).
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="ForeignTable">Foreign table object name</param>
    /// <param name="Columns">Column identifiers</param>
    public record ForeignKey(ObjectName ForeignTable, Sequence<Ident> Columns) : TableConstraint
    {
        public Ident? Name { get; init; }
        public Sequence<Ident>? ReferredColumns { get; init; }
        public ReferentialAction OnDelete { get; init; }
        public ReferentialAction OnUpdate { get; init; }
        public ConstraintCharacteristics? Characteristics { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteConstraint(Name);
            writer.WriteSql($"FOREIGN KEY ({Columns}) REFERENCES {ForeignTable}({ReferredColumns})");

            if (OnDelete != ReferentialAction.None)
            {
                writer.WriteSql($" ON DELETE {OnDelete}");
            }

            if (OnUpdate != ReferentialAction.None)
            {
                writer.WriteSql($" ON UPDATE {OnUpdate}");
            }

            if (Characteristics != null)
            {
                writer.WriteSql($" {Characteristics}");
            }
        }
    }

    /// <summary>
    /// Check constraint
    /// <example>
    /// <c>
    /// [ CONSTRAINT name ] CHECK (expr)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name">Name identifier</param>
    /// <param name="Expression">Check expression</param>
    public record Check(Expression Expression, Ident Name) : TableConstraint
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteConstraint(Name);
            writer.WriteSql($"CHECK ({Expression})");
        }
    }

    /// <summary>
    /// MySQLs [index definition][1] for index creation. Not present on ANSI so, for now, the usage
    /// is restricted to MySQL, as no other dialects that support this syntax were found.
    ///
    /// `{INDEX | KEY} [index_name] [index_type] (key_part,...) [index_option]...`
    ///
    /// <see href="https://dev.mysql.com/doc/refman/8.0/en/create-table.html"/>
    /// </summary>
    public record Index(Sequence<Ident> Columns) : TableConstraint
    {
        public bool DisplayAsKey { get; init; }
        public Ident? Name { get; init; }
        public IndexType IndexType { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            var displayAs = DisplayAsKey ? "KEY" : "INDEX";
            writer.Write(displayAs);

            if (Name != null)
            {
                writer.WriteSql($" {Name}");
            }

            if (IndexType != IndexType.None)
            {
                writer.WriteSql($" USING {IndexType}");
            }

            writer.WriteSql($" ({Columns})");
        }
    }

    /// <summary>
    /// Index constraint added as part of an alter table statement.
    /// As far as I know, Postgres specific.
    ///
    /// `[ CONSTRAINT constraint_name ] { UNIQUE | PRIMARY KEY } USING INDEX index_name [ DEFERRABLE | NOT DEFERRABLE ] [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]`
    ///
    /// <see href="https://www.postgresql.org/docs/current/sql-altertable.html"/>
    /// </summary>
    public record PostgresAlterTableIndex(Ident Name, Ident IndexName) : TableConstraint
    {
        public ConstraintCharacteristics? Characteristics { get; init; }
        public bool IsPrimaryKey { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("CONSTRAINT");
            writer.WriteSql($" {Name}");
            writer.Write(IsPrimaryKey ? " PRIMARY KEY" : " UNIQUE");
            writer.Write(" USING INDEX");
            writer.WriteSql($" {IndexName}");
            if (Characteristics != null)
            {
                writer.WriteSql($" {Characteristics}");
            }
        }
    }
    /// <summary>
    /// MySQLs [fulltext][1] definition. Since the [`SPATIAL`][2] definition is exactly the same,
    /// and MySQL displays both the same way, it is part of this definition as well.
    ///
    /// Supported syntax:
    ///
    /// {FULLTEXT | SPATIAL} [INDEX | KEY] [index_name] (key_part,...)
    ///
    /// key_part: col_name
    ///
    /// <see href="https://dev.mysql.com/doc/refman/8.0/en/fulltext-natural-language.html"/>
    /// <see href="https://dev.mysql.com/doc/refman/8.0/en/spatial-types.html"/>
    /// </summary>
    public record FulltextOrSpatial(Sequence<Ident> Columns) : TableConstraint
    {
        public bool FullText { get; init; }
        public KeyOrIndexDisplay IndexTypeDisplay { get; init; }
        public Ident? OptIndexName { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(FullText ? "FULLTEXT" : "SPATIAL");

            if (IndexTypeDisplay != KeyOrIndexDisplay.None)
            {
                writer.WriteSql($" {IndexTypeDisplay}");
            }

            if (OptIndexName != null)
            {
                writer.WriteSql($" {OptIndexName}");
            }

            writer.WriteSql($" ({Columns})");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}