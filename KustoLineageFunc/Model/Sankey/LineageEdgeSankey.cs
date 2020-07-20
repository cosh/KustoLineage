using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineageFunc.Model
{
    public class LineageEdgeSankey : AGraphObjectSankey
    {
        private readonly int _source;
        public int source
        {
            get { return _source; }

        }

        private readonly int _target;
        public int target
        {
            get { return _target; }

        }

        public LineageEdgeSankey(string id, string type, int mySource, int myTarget) : base(id, type)
        {
            _source = mySource;
            _target = myTarget;
        }
    }
}
