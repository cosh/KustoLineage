using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineageFunc.Model
{
    public class Lineage
    {
        private readonly object _lock = new object();

        private Dictionary<String, Database> _databases = new Dictionary<string, Database>();

        public Dictionary<string, Database> Databases { get => _databases; }

        private readonly string _clustername;

        public string Clustername
        {
            get { return _clustername; }
        }

        public Lineage(string clustername)
        {
            this._clustername = clustername;
        }

        public void AddUpdatePolicy(string databaseName, string tableName, UpdatePolicy policy)
        {
            lock (_lock)
            {
                if (_databases.ContainsKey(databaseName))
                {
                    _databases[databaseName].AddUpdatePolicy(tableName, policy);
                }
                else
                {
                    var database = new Database(databaseName);
                    database.AddUpdatePolicy(tableName, policy);

                    _databases.Add(databaseName, database);
                }
            }
        }

        public void AddTable(string databaseName, string tableName)
        {
            lock (_lock)
            {
                if (_databases.ContainsKey(databaseName))
                {
                    _databases[databaseName].AddTable(tableName);
                }
                else
                {
                    var database = new Database(databaseName);
                    database.AddTable(tableName);

                    _databases.Add(databaseName, database);
                }
            }
        }
    }
}
