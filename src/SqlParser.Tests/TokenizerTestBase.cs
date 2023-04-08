using FluentAssertions;
using SqlParser.Tokens;

namespace SqlParser.Tests;

public class TokenizerTestBase
{
    public static void Compare(IList<Token> expected, IList<Token> actual)
    {
        Assert.Equal(expected.Count, actual.Count);

        for (var i = 0; i < expected.Count; i++)
        {
            Assert.IsType(expected[i].GetType(), actual[i]);
            expected[i].Should().BeEquivalentTo(actual[i], options =>
            {
                // Exclude location since unit tests are often subsets of a larger query
                return options.RespectingRuntimeTypes().Excluding(t => t.Location);
            });
        }
    }
}