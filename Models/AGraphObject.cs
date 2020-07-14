using System.Collections.Generic;

namespace KustoLineage
{
    public abstract class AGraphObject
    {
        protected readonly object _lock = new object();

        private readonly string _id;
        public string Id
        {
            get { return _id; }
        }

        private readonly string _type;

        public string Type
        {
            get { return _type; }
        }

        private Dictionary<string, string> _properties;
        public Dictionary<string, string> Properties
        {
            get { return _properties; }
            set { _properties = value; }
        }

        public AGraphObject(string id, string type)
        {
            _id = id;
            _type = type;
            _properties = new Dictionary<string, string>();
        }

        public AGraphObject AddProperty(string key, string value)
        {
            _properties.TryAdd(key, value);

            return this;
        }
    }
}