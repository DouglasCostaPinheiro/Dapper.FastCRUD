using System;
using System.Linq;
using Dapper.FastCrud.EntityDescriptors;
using Dapper.FastCrud.Mappings;

namespace Dapper.FastCrud.SqlBuilders
{
    internal class OracleBuilder : GenericStatementSqlBuilder
    {
        public OracleBuilder(EntityDescriptor entityDescriptor, EntityMapping entityMapping) 
            : base(entityDescriptor, entityMapping, SqlDialect.Oracle)
        {
        }

        /// <summary>
        /// Constructs a full insert statement
        /// </summary>
        protected override string ConstructFullInsertStatementInternal()
        {
            if (this.RefreshOnInsertProperties.Length == 0)
            {
                return this.ResolveWithCultureInvariantFormatter($"INSERT INTO {this.GetTableName()} ({this.ConstructColumnEnumerationForInsert()}) VALUES ({this.ConstructParamEnumerationForInsert()})");
            }

            var id = Guid.NewGuid().ToString("N").Substring(0, 5);

            var plainTableName = this.EntityMapping.TableName;

            return this.ResolveWithCultureInvariantFormatter($@"
                DECLARE
                    {string.Join($";{Environment.NewLine}", this.RefreshOnInsertProperties.Select(propInfo => $"TYPE type_{propInfo.DatabaseColumnName} IS TABLE OF {this.GetTableName()}.{this.GetColumnName(propInfo.PropertyName)}%TYPE;{Environment.NewLine} val_{propInfo.DatabaseColumnName} type_{propInfo.DatabaseColumnName}"))};
                BEGIN
                   EXECUTE IMMEDIATE 'CREATE TABLE table_fastcrud_{id} AS (SELECT {this.ConstructRefreshOnInsertColumnSelection()} FROM {this.GetTableName()} WHERE 0=1)';

                   INSERT INTO {this.GetTableName()} ({this.ConstructColumnEnumerationForInsert()})
                   VALUES ({this.ConstructParamEnumerationForInsert()})
                   RETURNING {this.ConstructRefreshOnInsertColumnSelection()}
                        BULK COLLECT INTO {string.Join(",", this.RefreshOnInsertProperties.Select(propInfo => $"val_{propInfo.DatabaseColumnName}"))};
    
                    FOR indx IN 1 .. val_{this.RefreshOnInsertProperties.First().DatabaseColumnName}.COUNT
                    LOOP
                        EXECUTE IMMEDIATE 'INSERT INTO table_fastcrud_{id} ({this.ConstructRefreshOnInsertColumnSelection()}) VALUES ({string.Join(",", this.RefreshOnInsertProperties.Select(propInfo => $":{propInfo.DatabaseColumnName}"))})' USING {string.Join(",", this.RefreshOnInsertProperties.Select(propInfo => $"val_{propInfo.DatabaseColumnName}(indx)"))};
                    END LOOP;
                    :temp_table_fastcrud := 'table_fastcrud_{id}';
                END;
            ");
        }

        /// <summary>
        /// Constructs an update statement for a single entity.
        /// </summary>
        protected override string ConstructFullSingleUpdateStatementInternal()
        {
            if (this.RefreshOnUpdateProperties.Length == 0)
            {
                return base.ConstructFullSingleUpdateStatementInternal();
            }

            var id = Guid.NewGuid().ToString("N").Substring(0, 5);

            var plainTableName = this.EntityMapping.TableName;

            return this.ResolveWithCultureInvariantFormatter($@"
                DECLARE
                    {string.Join($";{Environment.NewLine}", this.RefreshOnInsertProperties.Select(propInfo => $"TYPE type_{propInfo.DatabaseColumnName} IS TABLE OF {this.GetTableName()}.{this.GetColumnName(propInfo.PropertyName)}%TYPE;{Environment.NewLine} val_{propInfo.DatabaseColumnName} type_{propInfo.DatabaseColumnName}"))};
                BEGIN
                   EXECUTE IMMEDIATE 'CREATE TABLE table_fastcrud_{id} AS (SELECT {this.ConstructRefreshOnUpdateColumnSelection()} FROM {this.GetTableName()} WHERE 0=1)';

                   UPDATE {this.GetTableName()} 
                   SET {this.ConstructUpdateClause()}
                   RETURNING {this.ConstructRefreshOnUpdateColumnSelection()}
                        BULK COLLECT INTO {string.Join(",", this.RefreshOnUpdateProperties.Select(propInfo => $"val_{propInfo.DatabaseColumnName}"))};
    
                    FOR indx IN 1 .. val_{this.RefreshOnUpdateProperties.First().DatabaseColumnName}.COUNT
                    LOOP
                        EXECUTE IMMEDIATE 'INSERT INTO table_fastcrud_{id} ({this.ConstructRefreshOnUpdateColumnSelection()}) VALUES ({string.Join(",", this.RefreshOnUpdateProperties.Select(propInfo => $":{propInfo.DatabaseColumnName}"))})' USING {string.Join(",", this.RefreshOnUpdateProperties.Select(propInfo => $"val_{propInfo.DatabaseColumnName}(indx)"))};
                    END LOOP;
                    :temp_table_fastcrud := 'table_fastcrud_{id}';
                END;
            ");

            /*var dbUpdatedOutputColumns = string.Join(",", this.RefreshOnUpdateProperties.Select(propInfo => $"inserted.{this.GetColumnName(propInfo, null, true)}"));
            var dbGeneratedColumns = string.Join(",", this.RefreshOnUpdateProperties.Select(propInfo => $"{this.GetColumnName(propInfo, null, true)}"));

            // the union will make the constraints be ignored
            return this.ResolveWithCultureInvariantFormatter($@"
                SELECT *
                    INTO #temp 
                    FROM (SELECT {dbGeneratedColumns} FROM {this.GetTableName()} WHERE 1=0 
                        UNION SELECT {dbGeneratedColumns} FROM {this.GetTableName()} WHERE 1=0) as u;

                UPDATE {this.GetTableName()} 
                    SET {this.ConstructUpdateClause()}
                    OUTPUT {dbUpdatedOutputColumns} INTO #temp
                    WHERE {this.ConstructKeysWhereClause()}

                SELECT * FROM #temp");*/
        }

        protected override string ConstructFullSelectStatementInternal(string selectClause, string fromClause, FormattableString whereClause = null, FormattableString orderClause = null, long? skipRowsCount = default(long?), long? limitRowsCount = default(long?), bool forceTableColumnResolution = false)
        {
            var idRNum = Guid.NewGuid().ToString("N").Substring(0, 5);

            var sql = this.ResolveWithCultureInvariantFormatter(
                $@"SELECT * FROM (
                    SELECT ROWNUM rnum{idRNum}, a.*
                    FROM (
                        SELECT {selectClause} FROM {fromClause}
                        {(whereClause != null ? $"WHERE {this.ResolveWithSqlFormatter(whereClause, forceTableColumnResolution)}" : null)}
                        {(orderClause != null ? $"ORDER BY {this.ResolveWithSqlFormatter(orderClause, forceTableColumnResolution)}" : null)}
                    ) a
                    {(limitRowsCount.HasValue ? $"WHERE ROWNUM <= {skipRowsCount ?? 0 + limitRowsCount ?? 0}" : null)}
                    {((limitRowsCount ?? 0) > 0 ? $"WHERE ROWNUM <= {skipRowsCount ?? 0 + limitRowsCount}" : "")}
                   ) WHERE rnum{idRNum} > {skipRowsCount ?? 0}");

            return sql;
        }
    }
}
