using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using KustoLineageFunc.Scraper;
using KustoLineageFunc.Transformer;

namespace KustoLineageFunc
{
    public static class LineageFunctionGraph
    {
        private static readonly string CLUSTERMETRIC = "cluster";

        [FunctionName("LineageFunctionGraph")]
        public static IActionResult Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "{clustername}/lineage/graph")] HttpRequest req,
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
            });

            var adxClient = ADXClient.Create(clustername, config, null);

            var scraper = new KustoScraper(adxClient);

            var lineage = scraper.Scrape(clustername);

            var result = GraphTransformer.Transfrom(lineage);

            return new OkObjectResult(JsonConvert.SerializeObject(result));
        }
    }
}
