
namespace SqlParser
{
    public class Location
    {
        public Location()
        {
            Line = 1; 
            Column = 1;
        }

        public static Location From(long line, long column)
        {
            return new Location
            {
                Line = line,
                Column = column
            };
        } 

        public Location NewLine()
        {
            Line += 1;
            Column = 1;

            return this;
        }

        public Location MoveCol()
        {
            Column += 1;

            return this;
        }

        public override string ToString()
        {
            return $"Line: {Line}, Col: {Column}";
        }

        public long Line { get; private set; }

        public long Column { get; private set; }
    }
}
