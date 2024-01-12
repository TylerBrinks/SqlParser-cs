using SqlParser.Ast;

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

            Assert.Equal(visitor.Elements.Count, visitor.Visited.Count / 2);
        }

        [Fact]
        public void Visitor_Uses_Order_Attribute_Value()
        {
            IElement cache = new Statement.Cache("name")
            {
                TableFlag = "flag",
                Options = new Sequence<SqlOption>(),
                Query = new Statement.Select(new Query(new SetExpression.SelectExpression(new Select(new Sequence<SelectItem>()))))
            };

            var properties = IElement.GetVisitableChildProperties(cache);

            Assert.Equal(nameof(Statement.Cache.TableFlag), properties[0].Name);
            Assert.Equal(nameof(Statement.Cache.Name), properties[1].Name);
            Assert.Equal(nameof(Statement.Cache.Options), properties[2].Name);
            Assert.Equal(nameof(Statement.Cache.Query), properties[3].Name);
        }
    }


    public class TestVisitor : Visitor
    {
        public List<string> Visited = new();
        public List<IElement> Elements = new();

        public override ControlFlow PostVisitRelation(TableFactor relation)
        {
            Elements.Add(relation);
            Visited.Add($"POST Relation: {relation.AsTable().Name}");
            return ControlFlow.Continue;
        }
        public override ControlFlow PostVisitExpression(Expression expression)
        {
            Elements.Add(expression);
            Visited.Add($"POST Expression: {expression.ToSql()}");
            return ControlFlow.Continue;
        }
        public override ControlFlow PostVisitStatement(Statement statement)
        {
            Elements.Add(statement);
            Visited.Add($"POST Statement: {statement.ToSql()}");
            return ControlFlow.Continue;
        }

        public override ControlFlow PreVisitStatement(Statement statement)
        {
            Visited.Add($"PRE Relation: {statement.ToSql()}");
            return ControlFlow.Continue;
        }
        public override ControlFlow PreVisitExpression(Expression expression)
        {
            Visited.Add($"PRE Expression: {expression.ToSql()}");
            return ControlFlow.Continue;
        }
        public override ControlFlow PreVisitRelation(TableFactor relation)
        {
            Visited.Add($"PRE Statement: {relation.AsTable().Name}");
            return ControlFlow.Continue;
        }

        //public override ControlFlow PreVisitTableFactor(TableFactor relation)
        //{
        //    Visited.Add($"PRE Table Factor: {relation.AsTable().Name}");
        //    return base.PreVisitTableFactor(statement);
        //}

        //public override ControlFlow PostVisitTableFactor(TableFactor relation)
        //{
        //    return base.PostVisitTableFactor(statement);
        //}
    }

}
