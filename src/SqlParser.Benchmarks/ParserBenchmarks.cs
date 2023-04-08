using BenchmarkDotNet.Attributes;
using SqlParser.Ast;

namespace SqlParser.Benchmarks;

[MemoryDiagnoser]
public class ParserBenchmarks
{
    private ParserOptions _options = null!;

    [Params(25, 50, 100)]
    public uint Depth;

    [GlobalSetup]
    public void Setup()
    {
        _options = new ParserOptions { RecursionLimit = Depth };
    }

    [Benchmark]
    public Sequence<Statement> Parse() => new Parser().ParseSql(Sql, _options);

    [Benchmark]
    public string ParseToSql() => new Parser().ParseSql(Sql, _options).ToSql();

    private const string Sql = """
        select
        	nation,
        	o_year,
        	sum(amount) as sum_profit
        from
        	(
        		select
        			n_name as nation,
        			extract(year from o_orderdate) as o_year,
        			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        		from
        			part,
        			supplier,
        			lineitem,
        			partsupp,
        			orders,
        			nation
        		where
        			s_suppkey = l_suppkey
        			and ps_suppkey = l_suppkey
        			and ps_partkey = l_partkey
        			and p_partkey = l_partkey
        			and o_orderkey = l_orderkey
        			and s_nationkey = n_nationkey
        			and p_name like '%green%'
        	) as profit
        group by
        	nation,
        	o_year
        order by
        	nation,
        	o_year desc;
        
        """;
}