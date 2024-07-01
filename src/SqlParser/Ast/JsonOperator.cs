namespace SqlParser.Ast;

public abstract record JsonPathElement
{
    public record Dot(string Key, bool Quoted) : JsonPathElement;
    public record Bracket(Expression Key) : JsonPathElement;
}

public record JsonPath(Sequence<JsonPathElement> Path) : IElement, IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        for (var i = 0; i < Path.Count; i++)
        {
            var element = Path[i];

            switch (element)
            {
                case JsonPathElement.Dot dot:
                {
                    writer.Write(i == 0 ? ":" : ".");

                    if (dot.Quoted)
                    {
                        writer.WriteSql($"\"{dot.Key.EscapeDoubleQuoteString()}\"");
                    }
                    else
                    {
                        writer.WriteSql($"{dot.Key}");
                    }

                    break;
                }
                case JsonPathElement.Bracket bracket:
                    writer.WriteSql($"[{bracket.Key}]");
                    break;
            }
        }
    }
}