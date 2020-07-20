using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineageFunc.Model
{
    public class LineageVertexSankey : AGraphObjectSankey
    {
        private readonly int _node;

        public int node => _node;

        public LineageVertexSankey(string id, int node, string type) : base(id, type)
        {
            _node = node;
        }
    }
}
