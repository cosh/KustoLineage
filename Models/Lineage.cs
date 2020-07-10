using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineage.Models
{
    public class Lineage
    {
        private readonly string _clustername;
        public string Clustername
        {
            get { return _clustername; }
        }

        private readonly string _region;
        public string Region
        {
            get { return _region; }
        }

        public Lineage(string clustername, string region)
        {
            this._clustername = clustername;
            this._region = region;
        }
    }
}
