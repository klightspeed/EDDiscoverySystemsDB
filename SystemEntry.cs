using System;
using System.Collections.Generic;
using System.Text;

namespace EDDiscoverySystemsDB
{
    public record struct SystemEntry
    {
        public long SystemAddress { get; init; }
        public long NameId { get; init; }
        public int SectorId { get; init; }
        public int X { get; init; }
        public int Y { get; init; }
        public int Z { get; init; }
        public int Info { get; init; }
    }
}
