using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineageFunc.Model
{
    public class InternalTable
    {
        private readonly string _name;

        public string Name { get => _name; }

        private long _rowCount;

        public long RowCount { get => _rowCount; set => _rowCount = value; }

        private HashSet<UpdatePolicy> _updatePolicies = new HashSet<UpdatePolicy>();

        public HashSet<UpdatePolicy> UpdatePolicies { get => _updatePolicies; }

        public InternalTable(string name)
        {
            _name = name;
        }

        public void AddUpdatePolicy(UpdatePolicy policy)
        {
            _updatePolicies.Add(policy);
        }

        public override bool Equals(object obj)
        {
            return obj is InternalTable table &&
                   Name == table.Name;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name);
        }
    }
}
