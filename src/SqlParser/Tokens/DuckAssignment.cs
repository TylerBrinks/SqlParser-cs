﻿namespace SqlParser.Tokens;

/// <summary>
/// Assignment `:=` (used for keyword argument in DuckDB macros)
/// </summary>
public class DuckAssignment : StringToken
{
    public DuckAssignment() : base(":=")
    {
    }
}