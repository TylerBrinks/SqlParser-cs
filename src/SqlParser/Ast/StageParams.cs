﻿namespace SqlParser.Ast;

/// <summary>
/// Snowflake stage params
/// </summary>
public record StageParams : IWriteSql, IElement
{
    public string? Url { get; init; }
    public string? Endpoint { get; init; }
    public string? StorageIntegration { get; init; }
    public Sequence<DataLoadingOption>? Credentials { get; init; }
    public Sequence<DataLoadingOption>? Encryption { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        if (Url != null)
        {
            writer.Write($" URL='{Url}'");
        }

        if (StorageIntegration != null)
        {
            writer.Write($" STORAGE_INTEGRATION={StorageIntegration}");
        }

        if (Endpoint != null)
        {
            writer.Write($" ENDPOINT='{Endpoint}'");
        }

        if (Credentials != null)
        {
            writer.WriteSql($" CREDENTIALS=({Credentials.ToSqlDelimited(Symbols.Space)})");
        }

        if (Encryption != null)
        {
            writer.WriteSql($" ENCRYPTION=({Encryption.ToSqlDelimited(Symbols.Space)})");
        }
    }
}

/// <summary>
/// Snowflake data loading options
/// </summary>
public record DataLoadingOption(string Name, DataLoadingOptionType OptionType, string Value) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.Write(OptionType == DataLoadingOptionType.String 
            ? $"{Name}='{Value}'" 
            : $"{Name}={Value}");
    }
}