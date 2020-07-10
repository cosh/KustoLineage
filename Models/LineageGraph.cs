using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineage.Models
{
    public class LineageGraph : AGraphObject
    {
        private List<LineageVertex> _vertices;
        public List<LineageVertex> Vertices
        {
            get { return _vertices; }
            set { _vertices = value; }
        }

        private List<LineageEdge> _edges;
        public List<LineageEdge> Edges
        {
            get { return _edges; }
            set { _edges = value; }
        }

        public LineageGraph(string id) : base(id)
        {
        }
    }
}
