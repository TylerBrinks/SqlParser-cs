using SqlParser.Ast;
using SqlParser.Dialects;

namespace SqlParser.Tests.Visitors
{
    public class VisitorTests
    {
        [Fact]
        public void Visitor_Finds_Properties()
        {
            var ast = new Parser().ParseSql("select * from abc as xyz");
            var visitor = new TestVisitor();

            ast.Visit(visitor);

            Assert.Equal(2, visitor.Elements.Count);
            Assert.Equal(8, visitor.Visited.Count);
        }

        [Fact]
        public void Visitor_Uses_Order_Attribute_Value()
        {
            IElement cache = new Statement.Cache("name")
            {
                TableFlag = "flag",
                Options = [],
                Query = new Statement.Select(new Query(new SetExpression.SelectExpression(new Select([]))))
            };

            var properties = IElement.GetVisitableChildProperties(cache);

            Assert.Equal(nameof(Statement.Cache.TableFlag), properties[0].Name);
            Assert.Equal(nameof(Statement.Cache.Name), properties[1].Name);
            Assert.Equal(nameof(Statement.Cache.Options), properties[2].Name);
            Assert.Equal(nameof(Statement.Cache.Query), properties[3].Name);
        }

        [Fact]
        public void Visitor_Visits_Sql_Parts()
        {
            #region Queries
            var queries = new Dictionary<string, List<string>>
            {
                {
                    "SELECT * from table_name as my_table", [
                        "PRE: STATEMENT: SELECT * FROM table_name AS my_table",
                        "PRE: QUERY: SELECT * FROM table_name AS my_table",
                        "PRE: TABLE FACTOR: table_name AS my_table",
                        "PRE: RELATION: table_name",
                        "POST: RELATION: table_name",
                        "POST: TABLE FACTOR: table_name AS my_table",
                        "POST: QUERY: SELECT * FROM table_name AS my_table",
                        "POST: STATEMENT: SELECT * FROM table_name AS my_table"
                    ]
                },
                {
                    "SELECT * from t1 join t2 on t1.id = t2.t1_id",
                    [
                        "PRE: STATEMENT: SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id",
                        "PRE: QUERY: SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id",
                        "PRE: TABLE FACTOR: t1",
                        "PRE: RELATION: t1",
                        "POST: RELATION: t1",
                        "POST: TABLE FACTOR: t1",
                        "PRE: TABLE FACTOR: t2",
                        "PRE: RELATION: t2",
                        "POST: RELATION: t2",
                        "POST: TABLE FACTOR: t2",
                        "PRE: EXPR: t1.id = t2.t1_id",
                        "PRE: EXPR: t1.id",
                        "POST: EXPR: t1.id",
                        "PRE: EXPR: t2.t1_id",
                        "POST: EXPR: t2.t1_id",
                        "POST: EXPR: t1.id = t2.t1_id",
                        "POST: QUERY: SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id",
                        "POST: STATEMENT: SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id"
                    ]
                },
                {
                    "SELECT * from t1 where EXISTS(SELECT column from t2)",
                    [
                        "PRE: STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2)",
                        "PRE: QUERY: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2)",
                        "PRE: TABLE FACTOR: t1",
                        "PRE: RELATION: t1",
                        "POST: RELATION: t1",
                        "POST: TABLE FACTOR: t1",
                        "PRE: EXPR: EXISTS (SELECT column FROM t2)",
                        "PRE: QUERY: SELECT column FROM t2",
                        "PRE: EXPR: column",
                        "POST: EXPR: column",
                        "PRE: TABLE FACTOR: t2",
                        "PRE: RELATION: t2",
                        "POST: RELATION: t2",
                        "POST: TABLE FACTOR: t2",
                        "POST: QUERY: SELECT column FROM t2",
                        "POST: EXPR: EXISTS (SELECT column FROM t2)",
                        "POST: QUERY: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2)",
                        "POST: STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2)"
                    ]
                },
                {
                    "SELECT * from t1 where EXISTS(SELECT column from t2) UNION SELECT * from t3",
                    [
                        "PRE: STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2) UNION SELECT * FROM t3",
                        "PRE: QUERY: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2) UNION SELECT * FROM t3",
                        "PRE: TABLE FACTOR: t1",
                        "PRE: RELATION: t1",
                        "POST: RELATION: t1",
                        "POST: TABLE FACTOR: t1",
                        "PRE: EXPR: EXISTS (SELECT column FROM t2)",
                        "PRE: QUERY: SELECT column FROM t2",
                        "PRE: EXPR: column",
                        "POST: EXPR: column",
                        "PRE: TABLE FACTOR: t2",
                        "PRE: RELATION: t2",
                        "POST: RELATION: t2",
                        "POST: TABLE FACTOR: t2",
                        "POST: QUERY: SELECT column FROM t2",
                        "POST: EXPR: EXISTS (SELECT column FROM t2)",
                        "PRE: TABLE FACTOR: t3",
                        "PRE: RELATION: t3",
                        "POST: RELATION: t3",
                        "POST: TABLE FACTOR: t3",
                        "POST: QUERY: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2) UNION SELECT * FROM t3",
                        "POST: STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2) UNION SELECT * FROM t3"
                    ]
                }
            };
            #endregion

            foreach (var query in queries)
            {
                var (visited, preVisitDefaultCalled, postVisitDefaultCalled) = Visit(query.Key);

                Assert.Equal(query.Value, visited);
                Assert.True(preVisitDefaultCalled);
                Assert.True(postVisitDefaultCalled);
            }

            static (List<string> visited, bool preVisitDefaultCalled, bool postVisitDefaultCalled) Visit(string sql)
            {
                var dialect = new GenericDialect();
                var parser = new Parser().TryWithSql(sql, dialect);
                var statements = parser.ParseStatements();
                var visitor = new TestVisitor();
                statements.Visit(visitor);
                return (visitor.Visited, visitor.PreVisitDefaultCalled, visitor.PostVisitDefaultCalled);
            }
        }
    }


    public class TestVisitor : Visitor
    {
        public List<string> Visited = [];
        public List<IElement> Elements = [];
        public bool PreVisitDefaultCalled = false;
        public bool PostVisitDefaultCalled = false;

        public override ControlFlow PreVisitStatement(Statement statement)
        {
            Visited.Add($"PRE: STATEMENT: {statement.ToSql()}");
            return ControlFlow.Continue;
        }
       
        public override ControlFlow PostVisitStatement(Statement statement)
        {
            Elements.Add(statement);
            Visited.Add($"POST: STATEMENT: {statement.ToSql()}");
            return ControlFlow.Continue;
        }

        //public override ControlFlow PreVisitRelation(TableFactor relation)
        public override ControlFlow PreVisitRelation(ObjectName relation)
        {
            Visited.Add($"PRE: RELATION: {relation}");
            return ControlFlow.Continue;
        }
        
        public override ControlFlow PostVisitRelation(ObjectName relation)
        {
            Elements.Add(relation);
            Visited.Add($"POST: RELATION: {relation}");
            return ControlFlow.Continue;
        }

        public override ControlFlow PreVisitExpression(Expression expression)
        {
            Visited.Add($"PRE: EXPR: {expression.ToSql()}");
            return ControlFlow.Continue;
        }
        
        public override ControlFlow PostVisitExpression(Expression expression)
        {
            Elements.Add(expression);
            Visited.Add($"POST: EXPR: {expression.ToSql()}");
            return ControlFlow.Continue;
        }

        public override ControlFlow PreVisitTableFactor(TableFactor tableFactor)
        {
            var sql = tableFactor.ToSql();
            Visited.Add($"PRE: TABLE FACTOR: {sql}");
            return ControlFlow.Continue;
        }

        public override ControlFlow PostVisitTableFactor(TableFactor tableFactor)
        {
            var sql = tableFactor.ToSql();
            Visited.Add($"POST: TABLE FACTOR: {sql}");
            return ControlFlow.Continue;
        }

        public override ControlFlow PreVisitQuery(Query query)
        {
            Visited.Add($"PRE: QUERY: {query.ToSql()}");
            return ControlFlow.Continue;
        }

        public override ControlFlow PostVisitQuery(Query query)
        {
            Visited.Add($"POST: QUERY: {query.ToSql()}");
            return ControlFlow.Continue;
        }

        public override ControlFlow PreVisitDefault(IElement element)
        {
            PreVisitDefaultCalled = true;
            return ControlFlow.Continue;
        }

        public override ControlFlow PostVisitDefault(IElement element)
        {
            PostVisitDefaultCalled = true;
            return ControlFlow.Continue;
        }
    }
}
