using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineageFunc
{
    public class ADXClient
    {
        public static ICslQueryProvider Create(string clusterName, IConfigurationRoot config, string userToken)
        {
            if (!string.IsNullOrWhiteSpace(userToken))
            {
                return KustoClientFactory.CreateCslQueryProvider(new KustoConnectionStringBuilder("https://" + clusterName + ".kusto.windows.net")
                    .WithAadUserTokenAuthentication(userToken));
            }
            else
            {
                return KustoClientFactory.CreateCslQueryProvider(new KustoConnectionStringBuilder("https://" + clusterName + ".kusto.windows.net")
                    .WithAadApplicationKeyAuthentication(
                            applicationClientId: GetEnvVariable(config, "ClientId"),
                            applicationKey: GetEnvVariable(config, "ClientSecret"),
                            authority: GetEnvVariable(config, "Tenant")));
            }
        }

        private static string GetEnvVariable(IConfigurationRoot config, String name)
        {
            return config[name];
        }
    }
}
