using KustoLineageFunc.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace KustoLineageFunc.Transformer
{
    public class GraphTransformer
    {
        public static LineageGraph Transfrom(Lineage lineage)
        {
            var result = new LineageGraph(Guid.NewGuid().ToString(), "lineage");

            result.AddProperty("executedOnUTC", DateTimeOffset.Now.ToUnixTimeMilliseconds().ToString());

            #region cluster node

            var rootNodeId = lineage.Clustername + "-" + Guid.NewGuid().ToString();

            var rootVertex = new LineageVertex(rootNodeId, "cluster");
            rootVertex
               .AddProperty("name", lineage.Clustername);

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

                var tableIdx = new Dictionary<String, String>();

                foreach (var aTable in aDatabase.Value.Tables)
                {
                    var tableVertexId = databaseVertexId + aTable.Key;

                    tableIdx.Add(aTable.Key, tableVertexId);

                    var tableVertex = new LineageVertex(tableVertexId, "table");
                    tableVertex
                       .AddProperty("name", aTable.Key);

                    result.AddVertex(tableVertex);

                    #region update policies

                    if (aTable.Value.UpdatePolicies.Count > 0)
                    {
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
                    }
                    else
                    {
                        var tableEdge = new LineageEdge(Guid.NewGuid().ToString(), "hasInternalTable", databaseVertexId, tableVertexId);

                        result.AddEdge(tableEdge);
                    }

                    #endregion
                }

                #endregion

                #region external tables
                
                var externalTableIdx = new Dictionary<String, String>();

                foreach (var anExternalTable in aDatabase.Value.ExternalTables)
                {
                    var externalTableVertexId = databaseVertexId + anExternalTable.Key;
                    externalTableIdx.Add(anExternalTable.Key, externalTableVertexId);
                }

                #endregion

                #region continous export

                var validExternalTableNames = new HashSet<String>();

                foreach (var aContinousExport in aDatabase.Value.ContinousExport)
                {
                    var ceEdgeId = databaseVertexId + aContinousExport.ExternalTableName;

                    var sourceTableName = ExtractTable(aContinousExport.CurserScopedQuery);

                    if(tableIdx.ContainsKey(sourceTableName) && externalTableIdx.ContainsKey(aContinousExport.ExternalTableName))
                    {
                        var sourceTable = tableIdx[ExtractTable(aContinousExport.CurserScopedQuery)];
                        var externalTable = externalTableIdx[aContinousExport.ExternalTableName];

                        if(!validExternalTableNames.Contains(aContinousExport.ExternalTableName))
                        {
                            var externalTableVertex = new LineageVertex(externalTable, "externalTable");
                            externalTableVertex
                               .AddProperty("name", aContinousExport.ExternalTableName);
                            result.AddVertex(externalTableVertex);

                            validExternalTableNames.Add(aContinousExport.ExternalTableName);
                        }

                        var ceEdge = new LineageEdge(ceEdgeId, "exportsTo", sourceTable, externalTable);

                        ceEdge.AddProperty("Query", aContinousExport.Query);
                        ceEdge.AddProperty("Name", aContinousExport.ExternalTableName);

                        result.AddEdge(ceEdge);
                    }
                }

                #endregion
            }

            #endregion

            return result;
        }

        private static string ExtractTable(string curserScopedQuery)
        {
            return curserScopedQuery.Split("].[")[1].Replace("\'", "").Replace("]", "");
        }
    }
}
