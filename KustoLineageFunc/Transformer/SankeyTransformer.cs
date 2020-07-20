using KustoLineageFunc.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Kusto.Cloud.Platform.Utils;

namespace KustoLineageFunc.Transformer
{
    public class SankeyTransformer
    {
        public static LineageGraphSankey Transform(LineageGraph graph)
        {
            LineageGraphSankey result = new LineageGraphSankey(graph.Id, graph.Type);

            graph.Properties.ForEach(akv => result.AddProperty(akv.Key, akv.Value));

            if(graph.Vertices.Count > 0)
            {
                Dictionary<string, int> idIdx = new Dictionary<string, int>();

                for (int i = 0; i < graph.Vertices.Count; i++)
                {
                    LineageVertex oldVertex = graph.Vertices[i];
                    LineageVertexSankey newVertex = new LineageVertexSankey(oldVertex.Id, i, oldVertex.Type);

                    newVertex.Properties = oldVertex.Properties;

                    result.AddVertex(newVertex);

                    idIdx.Add(oldVertex.Id, i);
                }

                if (graph.Edges.Count > 0)
                {
                    for (int i = 0; i < graph.Edges.Count; i++)
                    {
                        LineageEdge oldEdge = graph.Edges[i];
                        LineageEdgeSankey newEdge = new LineageEdgeSankey(oldEdge.Id, oldEdge.Type, idIdx[oldEdge.VSourceId], idIdx[oldEdge.VDestinationId]);

                        newEdge.Properties = oldEdge.Properties;

                        result.AddEdge(newEdge);
                    }
                }
            }

            return result;
        }
    }
}
