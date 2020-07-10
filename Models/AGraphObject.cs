using System.Collections.Generic;

namespace KustoLineage
{
    public abstract class AGraphObject
    {
        private readonly string _id;
        public string Id
        {
            get { return _id; }
        }

        private Dictionary<string, string> _properties;
        public Dictionary<string, string> Properties
        {
            get { return _properties; }
            set { _properties = value; }
        }

        public AGraphObject(string id)
        {
            _id = id;
            _properties = new Dictionary<string, string>();
        }

        public AGraphObject AddProperty(string key, string value)
        {
            _properties.TryAdd(key, value);

            return this;
        }
    }
}