﻿using Kusto.Data.Common;
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
            Dictionary<string, Task<IDataReader>> allTables = new Dictionary<string, Task<IDataReader>>();

            #region get Tables

            foreach (var aDatabase in databases)
            {
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