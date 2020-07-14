namespace KustoLineage.Models
{
    public class LineageEdge : AGraphObject
    {
        private readonly string _vSourceId;
        public string VSourceId
        {
            get { return _vSourceId; }
            
        }

        private readonly string _vDestinationId;
        public string VDestinationId
        {
            get { return _vDestinationId; }

        }

        public LineageEdge(string id, string type, string vSourceId, string vDestinationId) : base(id, type)
        {
            _vSourceId = vSourceId;
            _vDestinationId = vDestinationId;
        }
    }
}