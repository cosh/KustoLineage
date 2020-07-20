using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineageFunc.Model
{
    public class Database
    {
        private readonly object _lock = new object();

        private string _name;

        public string Name { get => _name; set => _name = value; }

        private Dictionary<String, InternalTable> _tables = new Dictionary<string, InternalTable>();

        public Dictionary<string, InternalTable> Tables { get => _tables; }

        public Database(string name)
        {
            _name = name;
        }

        public void AddUpdatePolicy(string table, UpdatePolicy policy)
        {
            lock (_lock)
            {
                if (_tables.ContainsKey(table))
                {
                    _tables[table].AddUpdatePolicy(policy);
                }
                else
                {
                    InternalTable internalTable = new InternalTable(table);
                    internalTable.AddUpdatePolicy(policy);
                    _tables.Add(table, internalTable);
                }
            }
        }

        public void AddTable(string tableName)
        {
            lock (_lock)
            {
                if (!_tables.ContainsKey(tableName))
                {
                    InternalTable internalTable = new InternalTable(tableName);
                    _tables.Add(tableName, internalTable);
                }
            }
        }
    }
}
