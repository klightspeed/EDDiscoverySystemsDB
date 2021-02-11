using System;
using System.Collections.Generic;
using System.Text;

namespace EDDiscoverySystemsDB
{
    public struct SystemEntry
    {
        public int EdsmId { get; set; }
        public int SectorId { get; set; }
        public long SystemAddress { get; set; }
        public long NameId { get; set; }
        public int X { get; set; }
        public int Y { get; set; }
        public int Z { get; set; }

        public static bool operator==(SystemEntry a, SystemEntry b)
        {
            return a.EdsmId == b.EdsmId &&
                   a.SectorId == b.SectorId &&
                   a.NameId == b.NameId &&
                   a.X == b.X &&
                   a.Y == b.Y &&
                   a.Z == b.Z;
        }

        public static bool operator!=(SystemEntry a, SystemEntry b)
        {
            return !(a == b);
        }

        public override bool Equals(object obj)
        {
            return obj is SystemEntry se && se == this;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(EdsmId, SectorId, NameId, X, Y, Z);
        }
    }
}
