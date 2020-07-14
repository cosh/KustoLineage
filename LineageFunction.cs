using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Kusto.Data;
using Kusto.Data.Net.Client;
using System.Data;
using System.Linq;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using Kusto.Data.Common;
using KustoLineage.Models;
using Kusto.Cloud.Platform.Data;

namespace KustoLineage
{
    public static class LineageFunction
    {
        private static readonly string CLUSTERMETRIC = "cluster";

        private static string GetEnvVariable(IConfigurationRoot config, String name)
        {
            return config[name];
        }

        [FunctionName("LineageFunction")]
        public static IActionResult Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "{region}/{clustername}/lineage")] HttpRequest req,
            string region,
            string clustername,
            ILogger log,
            ExecutionContext context)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            log.LogInformation("Executing lineage request");

            log.LogMetric(CLUSTERMETRIC, 1, new Dictionary<string, object>()
            {
                {"name", clustername },
                {"region", region }
            });

            var client = CreateADXClient(region, clustername, config);

            var lineage = new Lineage(clustername, region);

            AddLineageInformation(client, lineage);

            var graph = TransformToGraph(lineage);

            return new OkObjectResult(JsonConvert.SerializeObject(graph));
        }

        private static LineageGraph TransformToGraph(Lineage lineage)
        {
            var result = new LineageGraph(Guid.NewGuid().ToString(), "lineage");

            result.AddProperty("executedOnUTC", DateTimeOffset.Now.ToUnixTimeMilliseconds().ToString());

            #region cluster node

            var rootNodeId = lineage.Clustername + "." + lineage.Region;

            var rootVertex = new LineageVertex(rootNodeId, "cluster");
            rootVertex
               .AddProperty("name", lineage.Clustername)
               .AddProperty("region", lineage.Region);

            result.AddVertex(rootVertex);

            #endregion

            #region database nodes

            foreach (var aDatabase in lineage.Databases)
            {
                var databaseVertexId = aDatabase.Key;

                var databaseVertex = new LineageVertex(databaseVertexId, "database");
                databaseVertex
                   .AddProperty("name", aDatabase.Value.Name);

                result.AddVertex(databaseVertex);

                var databaseEdge = new LineageEdge(Guid.NewGuid().ToString(), "hasDatabase", rootNodeId, databaseVertexId);

                result.AddEdge(databaseEdge);

                #region table nodes

                foreach (var aTable in aDatabase.Value.Tables)
                {
                    var tableVertexId = databaseVertexId + aTable.Key;

                    var tableVertex = new LineageVertex(tableVertexId, "table");
                    tableVertex
                       .AddProperty("name", aTable.Key);

                    result.AddVertex(tableVertex);

                    var tableEdge = new LineageEdge(Guid.NewGuid().ToString(), "hasInternalTable", databaseVertexId, tableVertexId);

                    result.AddEdge(tableEdge);

                    #region update policies

                    foreach (var aUpdatePolicy in aTable.Value.UpdatePolicies)
                    {
                        var upEdgeId = Guid.NewGuid().ToString();
                        var sourceVertexId = databaseVertexId + aUpdatePolicy.Source;
                        var destinationVertexId = tableVertexId;

                        var upEdge = new LineageEdge(upEdgeId, "propagatesViaUp", sourceVertexId, destinationVertexId);

                        upEdge.AddProperty("IsEnabled", aUpdatePolicy.IsEnabled.ToString());
                        upEdge.AddProperty("IsTransactional", aUpdatePolicy.IsTransactional.ToString());
                        upEdge.AddProperty("PropagateIngestionProperties", aUpdatePolicy.PropagateIngestionProperties.ToString());
                        upEdge.AddProperty("Query", aUpdatePolicy.Query.ToString());

                        result.AddEdge(upEdge);
                    }

                    #endregion
                }

                #endregion
            }

            #endregion
            

            return result;
        }

        private static int AddLineageInformation(ICslQueryProvider adx, Lineage lineage)
        {
            var databases = adx.ExecuteQuery<String>(".show databases | project DatabaseName");

            List<Task<IDataReader>> allTablesQueries = new List<Task<IDataReader>>();
            Dictionary<string, Task<IDataReader>> allTables = new Dictionary<string, Task<IDataReader>>();
            
            int databaseCount = 0;

            #region get Tables

            foreach (var aDatabase in databases)
            {
                databaseCount++;

                var tableTask = adx.ExecuteQueryAsync(aDatabase, ".show tables | project TableName", CreateRequestProperties());

                allTablesQueries.Add(tableTask);
                allTables.Add(aDatabase, tableTask);
            }

            Task.WaitAll(allTablesQueries.ToArray());

            #endregion

            #region get update policies

            List<Task<IDataReader>> updatePolicyQueries = new List<Task<IDataReader>>();

            foreach (var aTableTask in allTables)
            {
                IDataReader tableResult = aTableTask.Value.Result;

                while (tableResult.Read())
                {
                    var tableName = tableResult.GetString(0);

                    lineage.AddTable(aTableTask.Key, tableName);

                    //get update policy
                    updatePolicyQueries.Add(adx.ExecuteQueryAsync(aTableTask.Key,
                        @".show table " + tableName + @" policy update
                            | mv-expand Policy=todynamic(Policy)
                            | project EntityName, Policy", 
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

            return databaseCount;
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

        private static ICslQueryProvider CreateADXClient(string region, string clustername, IConfigurationRoot config)
        {
            var connection =
                            new KustoConnectionStringBuilder("https://" + clustername + "." + region + ".kusto.windows.net").WithAadApplicationKeyAuthentication(
                            applicationClientId: GetEnvVariable(config, "ClientId"),
                            applicationKey: GetEnvVariable(config, "ClientSecret"),
                            authority: GetEnvVariable(config, "Tenant"));

            var adx = KustoClientFactory.CreateCslQueryProvider(connection);
            return adx;
        }
    }
}
