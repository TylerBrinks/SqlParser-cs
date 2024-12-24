using BenchmarkDotNet.Attributes;
using SqlParser.Ast;
using SqlParser.Dialects;

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
    public string ToSql() => new Parser().ParseSql(Sql, _options).ToSql();

    [Benchmark]
    public void PrefixParsingExceptionSuppression()
    {
        const string query = """
                    SELECT [e].[Id],
                           [e].[Cnpj],
                           [e].[CreatedAt],
                           [e].[Email],
                           [e].[Giin],
                           [e].[IsDeleted],
                           [e].[IsMultiSponsored],
                           [e].[Name],
                           [e].[UpdatedAt],
                           [e].[Uuid],
                           [t0].[Id],
                           [t0].[Cnpb],
                           [t0].[Cnpj],
                           [t0].[CreatedAt],
                           [t0].[EffectiveDate],
                           [t0].[EntityId],
                           [t0].[IsDeleted],
                           [t0].[Name],
                           [t0].[UpdatedAt],
                           [t0].[Uuid],
                           [t0].[Id0],
                           [t0].[CreatedAt0],
                           [t0].[EffectiveDate0],
                           [t0].[ExternalCode],
                           [t0].[IsDeleted0],
                           [t0].[Name0],
                           [t0].[PlanId],
                           [t0].[UpdatedAt0],
                           [t0].[Uuid0],
                           [t0].[Id00],
                           [t0].[CreatedAt00],
                           [t0].[ExternalCode0],
                           [t0].[IsDeleted00],
                           [t0].[Name00],
                           [t0].[SponsorId],
                           [t0].[UpdatedAt00],
                           [t0].[Uuid00]
                    FROM [Entities] AS [e]
                        LEFT JOIN
                        (
                            SELECT [p0].[Id],
                                   [p0].[Cnpb],
                                   [p0].[Cnpj],
                                   [p0].[CreatedAt],
                                   [p0].[EffectiveDate],
                                   [p0].[EntityId],
                                   [p0].[IsDeleted],
                                   [p0].[Name],
                                   [p0].[UpdatedAt],
                                   [p0].[Uuid],
                                   [t].[Id] AS [Id0],
                                   [t].[CreatedAt] AS [CreatedAt0],
                                   [t].[EffectiveDate] AS [EffectiveDate0],
                                   [t].[ExternalCode],
                                   [t].[IsDeleted] AS [IsDeleted0],
                                   [t].[Name] AS [Name0],
                                   [t].[PlanId],
                                   [t].[UpdatedAt] AS [UpdatedAt0],
                                   [t].[Uuid] AS [Uuid0],
                                   [t].[Id0] AS [Id00],
                                   [t].[CreatedAt0] AS [CreatedAt00],
                                   [t].[ExternalCode0],
                                   [t].[IsDeleted0] AS [IsDeleted00],
                                   [t].[Name0] AS [Name00],
                                   [t].[SponsorId],
                                   [t].[UpdatedAt0] AS [UpdatedAt00],
                                   [t].[Uuid0] AS [Uuid00]
                            FROM [Plans] AS [p0]
                                LEFT JOIN
                                (
                                    SELECT [s].[Id],
                                           [s].[CreatedAt],
                                           [s].[EffectiveDate],
                                           [s].[ExternalCode],
                                           [s].[IsDeleted],
                                           [s].[Name],
                                           [s].[PlanId],
                                           [s].[UpdatedAt],
                                           [s].[Uuid],
                                           [a].[Id] AS [Id0],
                                           [a].[CreatedAt] AS [CreatedAt0],
                                           [a].[ExternalCode] AS [ExternalCode0],
                                           [a].[IsDeleted] AS [IsDeleted0],
                                           [a].[Name] AS [Name0],
                                           [a].[SponsorId],
                                           [a].[UpdatedAt] AS [UpdatedAt0],
                                           [a].[Uuid] AS [Uuid0]
                                    FROM [Sponsors] AS [s]
                                        LEFT JOIN [Affiliates] AS [a]
                                            ON [s].[Id] = [a].[SponsorId]
                                ) AS [t]
                                    ON [p0].[Id] = [t].[PlanId]
                        ) AS [t0]
                            ON [e].[Id] = [t0].[EntityId]
                    WHERE [e].[Id] = @EntityId
                          AND EXISTS
                    (
                        SELECT 1
                        FROM [Plans] AS [p]
                        WHERE [e].[Id] = [p].[EntityId]
                              AND [p].[Id] = @PlanId
                    )
                    ORDER BY [e].[Id],
                             [t0].[Id],
                             [t0].[Id0]
                    """;

        var parsed = new Parser().ParseSql(query, new MsSqlDialect());
        Console.Write(parsed.ToSql());
    }

    [Benchmark]
    public void DataTypeParsingExceptionSuppression()
    {
        const string query = """
                             WITH DateCalculations AS (
                                 SELECT 
                                     CAST(DATEADD(MONTH, -2, GETDATE()) AS DATE) AS TwoMonthsAgo,
                                     CAST(DATEADD(MONTH, -1, GETDATE()) AS DATE) AS LastMonth,
                                     FORMAT(GETDATE(), 'yyyy-MM-dd') AS CurrentDateFormatted,
                                     FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyy-MM-dd') AS LastMonthFormatted,
                                     CAST(GETDATE() AS INT) AS CurrentDateInt,
                                     CONVERT(VARCHAR(7), GETDATE(), 120) AS CurrentYearMonth
                             ),
                             ParticipantStats AS (
                                 SELECT 
                                     p.Id AS ParticipantId,
                                     p.Name AS ParticipantName,
                                     COUNT(pm.Id) AS TotalMonthlyBalances,
                                     SUM(pm.GrossQuotaQuantity) AS TotalGrossQuotaQuantity,
                                     MAX(pm.ReferenceYearMonth) AS LastBalanceMonth
                                 FROM Participants p
                                 LEFT JOIN ParticipantMonthlyBalances pm
                                     ON p.Id = pm.ParticipantId
                                 GROUP BY p.Id, p.Name
                             )
                             SELECT 
                                 ps.ParticipantId,
                                 ps.ParticipantName,
                                 ps.TotalMonthlyBalances,
                                 ps.TotalGrossQuotaQuantity,
                                 ps.LastBalanceMonth,
                                 dc.TwoMonthsAgo,
                                 dc.LastMonth,
                                 dc.CurrentDateFormatted,
                                 dc.LastMonthFormatted,
                                 dc.CurrentDateInt,
                                 dc.CurrentYearMonth,
                                 CASE 
                                     WHEN ps.TotalGrossQuotaQuantity > 100 THEN 'High'
                                     WHEN ps.TotalGrossQuotaQuantity BETWEEN 50 AND 100 THEN 'Medium'
                                     ELSE 'Low'
                                 END AS QuotaCategory,
                                 FORMAT(GETDATE(), 'yyyy-MM-dd') AS TodayFormatted,
                                 CONVERT(VARCHAR(20), GETDATE(), 120) AS CurrentDateISO
                             FROM ParticipantStats ps
                             CROSS JOIN DateCalculations dc
                             WHERE ps.LastBalanceMonth BETWEEN dc.LastMonthFormatted AND dc.CurrentDateFormatted
                             ORDER BY ps.ParticipantName;
                             """;
        var parsed = new SqlQueryParser().Parse(query, new MsSqlDialect());
        Console.Write(parsed.ToSql());
    }

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