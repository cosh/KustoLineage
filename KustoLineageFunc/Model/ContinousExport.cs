using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineageFunc.Model
{
    public class ContinousExport
    {
        private string _continousExportName;
        private string _externalTableName;
        private string _query;
        private string _curserScopedQuery;

        public ContinousExport(string continousExportName, string externalTableName, string query, string curserScopedQuery)
        {
            this._continousExportName = continousExportName;
            this._externalTableName = externalTableName;
            this._query = query;
            this._curserScopedQuery = curserScopedQuery;
        }

        public string ContinousExportName { get => _continousExportName; set => _continousExportName = value; }
        public string ExternalTableName { get => _externalTableName; set => _externalTableName = value; }
        public string Query { get => _query; set => _query = value; }
        public string CurserScopedQuery { get => _curserScopedQuery; set => _curserScopedQuery = value; }
    }
}
