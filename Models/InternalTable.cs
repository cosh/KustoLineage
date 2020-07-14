using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace KustoLineage.Models
{
    public class InternalTable
    {
        private string _name;

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
                   _name == table._name;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(_name);
        }
    }
}
