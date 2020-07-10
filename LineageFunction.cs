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

namespace KustoLineage
{
    public static class LineageFunction
    {
        private static string GetEnvVariable(IConfigurationRoot config, String name)
        {
            return config[name];
        }

        [FunctionName("LineageFunction")]
        public static async Task<IActionResult> Run(
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

            log.LogInformation("C# HTTP trigger function processed a request.");

            var connection =
                new KustoConnectionStringBuilder("https://" + clustername + "." + region + ".kusto.windows.net").WithAadApplicationKeyAuthentication(
                applicationClientId: GetEnvVariable(config, "ClientId"),
                applicationKey: GetEnvVariable(config, "ClientSecret"),
                authority: GetEnvVariable(config, "Tenant"));

            var adx = KustoClientFactory.CreateCslQueryProvider(connection);

            IDataReader dataReader = adx.ExecuteQuery(".show queries | count");


            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            string responseMessage = string.IsNullOrEmpty(name)
                ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                : $"Hello, {name}. This HTTP triggered function executed successfully.";

            return new OkObjectResult(responseMessage);
        }
    }
}
