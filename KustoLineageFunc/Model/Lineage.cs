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
                    _databases[databaseName].AddInternalTable(tableName);
                }
                else
                {
                    var database = new Database(databaseName);
                    database.AddInternalTable(tableName);

                    _databases.Add(databaseName, database);
                }
            }
        }

        public void AddExternalTable(string databaseName, string tableName)
        {
            lock (_lock)
            {
                if (_databases.ContainsKey(databaseName))
                {
                    _databases[databaseName].AddExternalTable(tableName);
                }
                else
                {
                    var database = new Database(databaseName);
                    database.AddExternalTable(tableName);

                    _databases.Add(databaseName, database);
                }
            }
        }

        public void AddContinousExport(string databaseName, ContinousExport ce)
        {
            lock (_lock)
            {
                if (_databases.ContainsKey(databaseName))
                {
                    _databases[databaseName].AddContinousExport(ce);
                }
                else
                {
                    var database = new Database(databaseName);
                    database.AddContinousExport(ce);

                    _databases.Add(databaseName, database);
                }
            }
        }
    }
}
