using System.Runtime.CompilerServices;

namespace SqlParser;

[InterpolatedStringHandler]
public readonly ref struct SqlInterpolatedStringHandler
{
    private readonly SqlTextWriter _writer;

    /// <summary>
    /// Custom SQL string interpolation handler.  All formatted string segments will handle
    /// the ToSql invocation of any types implementing IWriteSql.  By default, all enumerable
    /// objects are written using a comma delimited format.
    /// </summary>
    public SqlInterpolatedStringHandler(int literalLength, int formattedCount, SqlTextWriter writer)
    {
        _writer = writer;
    }
    /// <summary>
    /// Typically an interpolation handler would build a string out of formatted
    /// segments.  However in the context of building a SQL string, there is already
    /// a string builder being used by a wrapper SqlTextWriter.  The writer is
    /// injected any time the WriteSql method is called, so the formatting and
    /// appending can be applied directly to the SqlTextWriter instance instead of
    /// building up a new string and returning it like a typical handler would.
    ///
    /// Doing so allows far fewer allocations
    /// </summary>
    /// <param name="value"></param>
    public void AppendLiteral(string? value) => _writer.Write(value);

    /// <summary>
    /// Invokes ToSql on any IWriteSql interpolated segments, and
    /// makes enumerations upper case
    /// </summary>
    /// <typeparam name="T">Type to write</typeparam>
    /// <param name="value">Value to append to the string</param>
    public void AppendFormatted<T>(T value)
    {
        switch (value)
        {
            case Enum e:
                AppendLiteral(EnumWriter.Write(e));
                break;

            case IWriteSql sql:
                sql.ToSql(_writer);
                break;

            case string str:
                // No need to write null or empty strings when the interpolated
                // or formatted segment yields null or empty .
                if (!string.IsNullOrEmpty(str))
                {
                    AppendLiteral(str);
                }
                break;

            default:
                if (value != null)
                {
                    AppendFormatted(value.ToString());
                }

                break;
        }
    }
}