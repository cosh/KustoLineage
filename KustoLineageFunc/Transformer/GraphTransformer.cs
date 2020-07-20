using KustoLineageFunc.Model;
using System;
using System.Collections.Generic;
using System.Text;

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
    }
}
