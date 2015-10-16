﻿namespace Dapper.FastCrud.SqlBuilders
{
    using System;
    using System.Globalization;
    using System.Linq;
    using Dapper.FastCrud.EntityDescriptors;
    using Dapper.FastCrud.Mappings;

    internal class MySqlBuilder:GenericStatementSqlBuilder
    {
        public MySqlBuilder(
            SqlDialectConfiguration configuration,
            EntityDescriptor entityDescriptor,
            EntityMapping entityMapping)
            : base(
                  entityDescriptor, 
                  entityMapping, 
                  configuration.IsUsingSchemas,
                  configuration.IdentifierStartDelimiter,
                  configuration.IdentifierEndDelimiter)
        {
        }

        public override string ConstructFullInsertStatement()
        {
            var sql = $"INSERT INTO {this.GetTableName()} ({this.ConstructColumnEnumerationForInsert()}) VALUES ({this.ConstructParamEnumerationForInsert()}); ";

            if (this.InsertKeyDatabaseGeneratedProperties.Length > 0)
            {
                // we have an identity column, so we can fetch the rest of them
                if (this.InsertKeyDatabaseGeneratedProperties.Length == 1 && this.InsertDatabaseGeneratedProperties.Length == 1)
                {
                    // just one, this is going to be easy
                    sql += $"SELECT LAST_INSERT_ID() as {this.GetDelimitedIdentifier(this.InsertKeyDatabaseGeneratedProperties[0].PropertyName)}";
                }
                else
                {
                    var databaseGeneratedColumnSelection = string.Join(
                        ",",
                        this.InsertDatabaseGeneratedProperties.Select(
                            propInfo =>
                            $"{this.GetColumnName(propInfo, null, true)}"));
                    sql +=
                        $"SELECT {databaseGeneratedColumnSelection} FROM {this.GetTableName()} WHERE {this.GetColumnName(this.InsertKeyDatabaseGeneratedProperties[0],null,false)} = LAST_INSERT_ID()";
                }
            }
            else if(this.InsertDatabaseGeneratedProperties.Length > 0)
            {
                throw new NotSupportedException($"Entity '{this.EntityMapping.EntityType.Name}' has database generated fields that don't contain a primary key. Either mark the primary key as database generated or remove the database generated flag from all the fields or mark all these fields as included in the insert operation.");
            }
            return sql;
        }

        public override string ConstructFullBatchSelectStatement(
            FormattableString whereClause = null,
            FormattableString orderClause = null,
            int? skipRowsCount = null,
            int? limitRowsCount = null,
            object queryParameters = null)
        {
            var sql = $"SELECT {this.ConstructColumnEnumerationForSelect()} FROM {this.GetTableName()}";

            if (whereClause != null)
            {
                sql += string.Format(this.StatementFormatter, " WHERE {0}", whereClause);
            }
            if (orderClause != null)
            {
                sql += string.Format(this.StatementFormatter, " ORDER BY {0}", orderClause);
            }

            if (skipRowsCount.HasValue)
            {
                sql += string.Format(CultureInfo.InvariantCulture, " LIMIT {0},{1}", skipRowsCount, limitRowsCount ?? (int?)int.MaxValue);
            }
            else if (limitRowsCount.HasValue)
            {
                sql += string.Format(CultureInfo.InvariantCulture, " LIMIT {0}", limitRowsCount);
            }

            return sql;
        }
    }
}
