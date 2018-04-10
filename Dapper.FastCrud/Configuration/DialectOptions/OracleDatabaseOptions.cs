namespace Dapper.FastCrud.Configuration.DialectOptions
{
    internal class OracleDatabaseOptions : SqlDatabaseOptions
    {
        public OracleDatabaseOptions()
        {
            this.StartDelimiter = this.EndDelimiter = "\"";
            this.IsUsingSchemas = true;
            this.ParameterPrefix = ":";
        }
    }
}
