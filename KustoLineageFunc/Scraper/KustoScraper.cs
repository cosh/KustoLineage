using Kusto.Cloud.Platform.Data;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data.Common;
using KustoLineageFunc.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace KustoLineageFunc.Scraper
{
    public class KustoScraper
    {
        private readonly ICslQueryProvider queryProvider;

        public KustoScraper(ICslQueryProvider queryProvider)
        {
            this.queryProvider = queryProvider;
        }

        public Lineage Scrape(string clusterName)
        {
            var lineage = new Lineage(clusterName);

            AddLineageInformation(queryProvider, lineage);

            return lineage;
        }

        private static void AddLineageInformation(ICslQueryProvider adx, Lineage lineage)
        {
            var databases = adx.ExecuteQuery<String>(".show databases | project DatabaseName");

            List<Task<IDataReader>> allTablesQueries = new List<Task<IDataReader>>();
            List<Task<IDataReader>> externalTableQueries = new List<Task<IDataReader>>();
            List<Task<IDataReader>> continousExportQueries = new List<Task<IDataReader>>();
            //Dictionary<string, Task<IDataReader>> allTables = new Dictionary<string, Task<IDataReader>>();

            #region get Tables

            foreach (var aDatabase in databases)
            {
                #region internal tables
                var tableTask = adx.ExecuteQueryAsync(aDatabase, ".show tables | project TableName, Database=current_database()", CreateRequestProperties());

                allTablesQueries.Add(tableTask);
                //allTables.Add(aDatabase, tableTask);
                #endregion

                #region external tables
                externalTableQueries.Add(adx.ExecuteQueryAsync(aDatabase,
                    @".show external tables
                        | project TableName, Database=current_database()",
                    CreateRequestProperties()));

                continousExportQueries.Add(adx.ExecuteQueryAsync(aDatabase,
                    @".show continuous-exports 
                        | project Name, ExternalTableName, Query, CursorScopedTables=todynamic(CursorScopedTables), Database=current_database()
                        | mv-expand CursorScopedTables to typeof(string)",
                    CreateRequestProperties()));

                #endregion
            }

            Task.WaitAll(allTablesQueries.ToArray());

            #endregion

            List<Task<IDataReader>> updatePolicyQueries = new List<Task<IDataReader>>();
            List<Task<IDataReader>> tableDetailsQueries = new List<Task<IDataReader>>();

            #region get update policies

            foreach (var aTableTask in allTablesQueries)
            {
                IDataReader tableResult = aTableTask.Result;

                while (tableResult.Read())
                {
                    var tableName = tableResult.GetString(0);

                    var databaseName = tableResult.GetString(1);

                    lineage.AddTable(databaseName, tableName);

                    //get update policy
                    updatePolicyQueries.Add(adx.ExecuteQueryAsync(databaseName,
                        @".show table " + tableName + @" policy update
                            | mv-expand Policy=todynamic(Policy)
                            | project EntityName, Policy",
                        CreateRequestProperties()));

                    //get table details
                    tableDetailsQueries.Add(adx.ExecuteQueryAsync(databaseName,
                        @".show table " + tableName + @" details
                            | project-away *Policy, AuthorizedPrincipals",
                        CreateRequestProperties()));
                }
            }

            Task.WaitAll(updatePolicyQueries.ToArray());
            
            foreach (var aTask in updatePolicyQueries)
            {
                IDataReader updatePolicyResult = aTask.Result;

                while (updatePolicyResult.Read())
                {

                    var policy = JsonConvert.DeserializeObject<UpdatePolicy>(updatePolicyResult["Policy"].ToString());
                    var Entity = updatePolicyResult.GetString(0);

                    var entitySplit = Entity.Split("].[");
                    var database = entitySplit[0].Replace("[", "");
                    var table = entitySplit[1].Replace("]", "");

                    lineage.AddUpdatePolicy(database, table, policy);
                }
            }

            #endregion

            #region table details

            Task.WaitAll(tableDetailsQueries.ToArray());

            foreach (var aTask in tableDetailsQueries)
            {
                IDataReader tableDetailResult = aTask.Result;

                while (tableDetailResult.Read())
                {
                    var database = tableDetailResult.GetString(1);
                    var table = tableDetailResult.GetString(0);

                    var rowCount = tableDetailResult.GetInt64(7);

                    lineage.Databases[database].Tables[table].RowCount = rowCount;
                }
            }

            #endregion

            #region external tables

            Task.WaitAll(externalTableQueries.ToArray());

            foreach (var aTask in externalTableQueries)
            {
                IDataReader externalTableResult = aTask.Result;

                while (externalTableResult.Read())
                {
                    var tableName = externalTableResult.GetString(0);

                    var databaseName = externalTableResult.GetString(1);

                    lineage.AddExternalTable(databaseName, tableName);
                }
            }

            #endregion

            #region continous export

            Task.WaitAll(continousExportQueries.ToArray());

            foreach (var aTask in continousExportQueries)
            {
                IDataReader continousExportResult = aTask.Result;

                while (continousExportResult.Read())
                {
                    var continousExportName = continousExportResult.GetString(0);

                    var externalTableName = continousExportResult.GetString(1);

                    var query = continousExportResult.GetString(2);

                    var curserScopedQuery = continousExportResult.GetString(3);

                    var databaseName = continousExportResult.GetString(4);

                    var ce = new ContinousExport(continousExportName, externalTableName, query, curserScopedQuery);

                    lineage.AddContinousExport(databaseName, ce);
                }
            }

            #endregion
        }

        private static ClientRequestProperties CreateRequestProperties()
        {
            var queryParameters = new Dictionary<String, String>()
            {
                //{ "xIntValue", "111" },
                // { "xStrValue", "abc" },
                // { "xDoubleValue", "11.1" }
            };

            var clientRequestProperties = new ClientRequestProperties(
                options: null,
                parameters: queryParameters)
            {
                ClientRequestId = "LINEAGE-" + Guid.NewGuid().ToString()
            };
            return clientRequestProperties;
        }
    }
}
