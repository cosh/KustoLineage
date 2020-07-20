using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineageFunc.Model
{
    public class LineageGraphSankey : AGraphObjectSankey
    {
        private List<LineageVertexSankey> _nodes = new List<LineageVertexSankey>();
        public List<LineageVertexSankey> nodes
        {
            get { return _nodes; }
            set { _nodes = value; }
        }

        private List<LineageEdgeSankey> _links = new List<LineageEdgeSankey>();
        public List<LineageEdgeSankey> links
        {
            get { return _links; }
            set { _links = value; }
        }

        public LineageGraphSankey(string id, string type) : base(id, type)
        {
        }

        public void AddEdge(LineageEdgeSankey edge)
        {
            lock (_lock)
            {
                _links.Add(edge);
            }
        }

        public void AddVertex(LineageVertexSankey vertex)
        {
            lock (_lock)
            {
                _nodes.Add(vertex);
            }
        }
    }
}
