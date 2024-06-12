namespace SqlParser.Ast;

public record Measure(Expression Expression, Ident Alias) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Expression} AS {Alias}");
    }
}

public abstract record RowsPerMatch : IWriteSql, IElement
{
    public record OneRow : RowsPerMatch;

    public record AllRows(EmptyMatchesMode? Mode) : RowsPerMatch;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case OneRow:
                writer.Write("ONE ROW PER MATCH");
                break;

            case AllRows a:
                writer.Write("ALL ROWS PER MATCH");
                if (a.Mode != null)
                {
                    writer.WriteSql($" {a.Mode}");
                }
                break;
        }
    }
}

public abstract record AfterMatchSkip : IWriteSql, IElement
{
    public record PastLastRow : AfterMatchSkip;
    public record ToNextRow : AfterMatchSkip;
    public record ToFirst(Ident Symbol) : AfterMatchSkip;
    public record ToLast(Ident Symbol) : AfterMatchSkip;

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("AFTER MATCH SKIP ");

        switch (this)
        {
            case PastLastRow:
                writer.Write("PAST LAST ROW");
                break;
            case ToNextRow:
                writer.Write(" TO NEXT ROW");
                break;
            case ToFirst f:
                writer.WriteSql($"TO FIRST {f.Symbol}");
                break;
            case ToLast l:
                writer.WriteSql($"TO LAST {l.Symbol}");
                break;
        }
    }
}

public enum EmptyMatchesMode
{
    Show,
    Omit,
    WithUnmatched,
}

public record SymbolDefinition(Ident Symbol, Expression Definition) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Symbol} AS {Definition}");
    }
}

public abstract record MatchRecognizeSymbol : IWriteSql, IElement
{
    public record Named(Ident Symbol) : MatchRecognizeSymbol;
    public record Start: MatchRecognizeSymbol;
    public record End: MatchRecognizeSymbol;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Named n:
                writer.WriteSql($"{n.Symbol}");
                break;
            case Start:
                writer.Write("^");
                break;
            case End:
                writer.Write("$");
                break;
        }
    }
}

public abstract record MatchRecognizePattern : IWriteSql, IElement
{
    public record Symbol(MatchRecognizeSymbol MatchSymbol) : MatchRecognizePattern;
    public record Exclude(MatchRecognizeSymbol ExcludeSymbol) : MatchRecognizePattern;
    public record Permute(Sequence<MatchRecognizeSymbol> Symbols) : MatchRecognizePattern;
    public record Concat(Sequence<MatchRecognizePattern> Patterns) : MatchRecognizePattern;
    public record Group(MatchRecognizePattern Pattern) : MatchRecognizePattern;
    public record Alternation(Sequence<MatchRecognizePattern> Patterns) : MatchRecognizePattern;
    public record Repetition(MatchRecognizePattern Pattern, RepetitionQualifier Operation) : MatchRecognizePattern;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Symbol s:
                writer.WriteSql($"{s.MatchSymbol}");
                break;
            case Exclude e:
                writer.WriteSql($"{{- {e.ExcludeSymbol} -}}");
                break;
            case Permute p:
                writer.Write($"PERMUTE({p.Symbols.ToSqlDelimited()})");
                break;
            case Concat c:
                writer.WriteDelimited(c.Patterns, Symbols.Space);
                break;
            case Group g:
                writer.WriteSql($"( {g.Pattern} )");
                break;
            case Alternation a:
                writer.WriteDelimited(a.Patterns, " | ");
                break;
            case Repetition r:
                writer.WriteSql($"{r.Pattern}{r.Operation}");
                break;
        }
    }
}

public abstract record RepetitionQualifier : IWriteSql, IElement
{
    public record ZeroOrMore : RepetitionQualifier;
    public record OneOrMore : RepetitionQualifier;
    public record AtMostOne : RepetitionQualifier;
    public record Exactly(int Value) : RepetitionQualifier;
    public record AtLeast(int Value) : RepetitionQualifier;
    public record AtMost(int Value) : RepetitionQualifier;
    public record Range(int Min, int Max) : RepetitionQualifier;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case ZeroOrMore:
                writer.Write("*");
                break;
            case OneOrMore:
                writer.Write("+");
                break;
            case AtMostOne:
                writer.Write("?");
                break;
            case Exactly e:
                writer.WriteSql($"{{{e.Value}}}");
                break;
            case AtLeast al:
                writer.WriteSql($"{{{al.Value},}}");
                break;
            case AtMost am:
                writer.WriteSql($"{{,{am.Value}}}");
                break;
            case Range r:
                writer.WriteSql($"{{{r.Min},{r.Max}}}");
                break;
        }
    }
}