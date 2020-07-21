using System;
using System.Collections.Generic;
using System.Text;

namespace KustoLineageFunc.Model
{
    public class ContinousExport
    {
        private string continousExportName;
        private string externalTableName;
        private string query;
        private string curserScopedQuery;

        public ContinousExport(string continousExportName, string externalTableName, string query, string curserScopedQuery)
        {
            this.continousExportName = continousExportName;
            this.externalTableName = externalTableName;
            this.query = query;
            this.curserScopedQuery = curserScopedQuery;
        }
    }
}
