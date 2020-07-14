using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineage.Models
{
    public class LineageGraph : AGraphObject
    {
        private List<LineageVertex> _vertices = new List<LineageVertex>();
        public List<LineageVertex> Vertices
        {
            get { return _vertices; }
            set { _vertices = value; }
        }

        private List<LineageEdge> _edges = new List<LineageEdge>();
        public List<LineageEdge> Edges
        {
            get { return _edges; }
            set { _edges = value; }
        }

        public LineageGraph(string id, string type) : base(id, type)
        {
        }

        public void AddVertex(string vertexId, string type)
        {
            lock (_lock)
            {
                var vertex = new LineageVertex(vertexId, type);
                _vertices.Add(vertex);
            }
        }

        public void AddEdge(LineageEdge edge)
        {
            lock (_lock)
            {
                _edges.Add(edge);
            }
        }

        public void AddVertex(LineageVertex updatePolicyVertex)
        {
            lock(_lock)
            {
                _vertices.Add(updatePolicyVertex);
            }
        }
    }
}
