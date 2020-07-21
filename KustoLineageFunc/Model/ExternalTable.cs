using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineageFunc.Model
{
    public class ExternalTable
    {
        private readonly string _name;

        public string Name { get => _name; }
        
        public ExternalTable(string name)
        {
            _name = name;
        }
    }
}
