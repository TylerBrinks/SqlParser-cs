namespace SqlParser.Ast;

/// <summary>
/// Grand objects
/// </summary>
public abstract record GrantObjects(Sequence<ObjectName> Schemas) : IWriteSql, IElement
{
    /// <summary>
    /// Grant privileges on ALL SEQUENCES IN SCHEMA schema_name [, ...]
    /// </summary>
    /// <param name="Schemas">Schemas</param>
    public record AllSequencesInSchema(Sequence<ObjectName> Schemas) : GrantObjects(Schemas);
    /// <summary>
    /// Grant privileges on ALL TABLES IN SCHEMA schema_name [, ...]
    /// </summary>
    /// <param name="Schemas">Schemas</param>
    public record AllTablesInSchema(Sequence<ObjectName> Schemas) : GrantObjects(Schemas);
    /// <summary>
    /// Grant privileges on specific schemas
    /// </summary>
    /// <param name="Schemas">Schemas</param>
    public record Schema(Sequence<ObjectName> Schemas) : GrantObjects(Schemas);
    /// <summary>
    /// Grant privileges on specific sequences
    /// </summary>
    /// <param name="Schemas">Schemas</param>
    public record Sequences(Sequence<ObjectName> Schemas) : GrantObjects(Schemas);
    /// <summary>
    /// Grant privileges on specific tables
    /// </summary>
    /// <param name="Schemas">Schemas</param>
    public record Tables(Sequence<ObjectName> Schemas) : GrantObjects(Schemas);

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Sequences:
                writer.WriteSql($"SEQUENCE {Schemas}");
                break;

            case Schema:
                writer.WriteSql($"SCHEMA {Schemas}");
                break;

            case Tables:
                writer.WriteSql($"{Schemas}");
                break;

            case AllSequencesInSchema:
                writer.WriteSql($"ALL SEQUENCES IN SCHEMA {Schemas}");
                break;

            case AllTablesInSchema:
                writer.WriteSql($"ALL TABLES IN SCHEMA {Schemas}");
                break;
        }
    }
}