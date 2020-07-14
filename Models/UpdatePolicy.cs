using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineage.Models
{
    public class UpdatePolicy
    {
        private bool _isEnabled;
        private String _source;
        private String _query;
        private bool _isTransactional;
        private string _propagateIngestionProperties;

        public bool IsEnabled { get => _isEnabled; set => _isEnabled = value; }
        public string Source { get => _source; set => _source = value; }
        public string Query { get => _query; set => _query = value; }
        public bool IsTransactional { get => _isTransactional; set => _isTransactional = value; }
        public string PropagateIngestionProperties { get => _propagateIngestionProperties; set => _propagateIngestionProperties = value; }

        public override bool Equals(object obj)
        {
            return obj is UpdatePolicy policy &&
                   _source == policy._source &&
                   _query == policy._query;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(_source, _query);
        }
    }
}
