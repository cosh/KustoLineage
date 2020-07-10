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
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using Kusto.Data.Common;
using KustoLineage.Models;

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
            var result = new LineageGraph(Guid.NewGuid().ToString());

            result
                .AddProperty("cluster", lineage.Clustername)
                .AddProperty("region", lineage.Region);

            return result;
        }

        private static int AddLineageInformation(ICslQueryProvider adx, Lineage lineage)
        {
            var databases = adx.ExecuteQuery<String>(".show databases | project DatabaseName");

            List<Task<IDataReader>> allTablesQueries = new List<Task<IDataReader>>();
            Dictionary<string, Task<IDataReader>> allTables = new Dictionary<string, Task<IDataReader>>();
            
            int databaseCount = 0;
            foreach (var aDatabase in databases)
            {
                databaseCount++;

                var tableTask = adx.ExecuteQueryAsync(aDatabase, ".show tables | project TableName", CreateRequestProperties());

                allTablesQueries.Add(tableTask);
                allTables.Add(aDatabase, tableTask);
            }

            Task.WaitAll(allTablesQueries.ToArray());

            foreach (var aTableTask in allTables)
            {
                var database = aTableTask;
                IDataReader tableResult = aTableTask.Value.Result;

                while (tableResult.Read())
                {
                    var tableName = tableResult.GetString(0);

                    //get update policy
                    //then: lineage.AddTable(tableName);
                }
            }

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
