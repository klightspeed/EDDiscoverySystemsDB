using ICSharpCode.SharpZipLib.GZip;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;

namespace EDDiscoverySystemsDB
{
    public class SystemsDB
    {
        public string BasePath { get; set; } = ".";

        private static readonly HashSet<string> Surveys = new(StringComparer.OrdinalIgnoreCase)
        {
            "2MASS",
            "HD",
            "LTT",
            "TYC",
            "NGC",
            "HR",
            "LFT",
            "LHS",
            "LP",
            "Wolf",
            "IHA2007",
            "USNO-A2.0",
            "2547",
            "DBP2006",
            "NOMAD1",
            "OJV2009",
            "PSR",
            "SSTGLMC",
            "StKM",
            "UGCS"
        };

        private static readonly int[] GridZ = new[]
        {
             0,
             2,
             4,
             5,
             6,
             7,
             8,
             9,
            10,
            11,
            12,
            13,
            15,
            17,
            19,
            24,
            29,
            34,
            39,
            44,
            49,
            54,
            59,
            64,
            69
        };

        private static readonly int[] GridX = new[]
        {
             0,
             4,
             9,
            11,
            13,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            25,
            27,
            29,
            34,
            39
        };

        private static readonly Dictionary<string, EDStar> SpanshToEDStar = new(StringComparer.OrdinalIgnoreCase)
        {
            { "O (Blue-White) Star", EDStar.O },
            { "B (Blue-White) Star", EDStar.B },
            { "A (Blue-White) Star", EDStar.A },
            { "F (White) Star", EDStar.F },
            { "G (White-Yellow) Star", EDStar.G },
            { "K (Yellow-Orange) Star", EDStar.K },
            { "M (Red dwarf) Star", EDStar.M },
            { "L (Brown dwarf) Star", EDStar.L },
            { "T (Brown dwarf) Star", EDStar.T },
            { "Y (Brown dwarf) Star", EDStar.Y },
            { "Herbig Ae Be Star", EDStar.AeBe },
            { "Herbig Ae/Be Star", EDStar.AeBe },
            { "T Tauri Star", EDStar.TTS },
            { "Wolf-Rayet Star", EDStar.W },
            { "Wolf-Rayet N Star", EDStar.WN },
            { "Wolf-Rayet NC Star", EDStar.WNC },
            { "Wolf-Rayet C Star", EDStar.WC },
            { "Wolf-Rayet O Star", EDStar.WO },
            { "C Star", EDStar.C },
            { "CN Star", EDStar.CN },
            { "CJ Star", EDStar.CJ },
            { "MS-type Star", EDStar.MS },
            { "S-type Star", EDStar.S },
            { "White Dwarf (D) Star", EDStar.D },
            { "White Dwarf (DA) Star", EDStar.DA },
            { "White Dwarf (DAB) Star", EDStar.DAB },
            { "White Dwarf (DAZ) Star", EDStar.DAZ },
            { "White Dwarf (DAV) Star", EDStar.DAV },
            { "White Dwarf (DB) Star", EDStar.DB },
            { "White Dwarf (DBZ) Star", EDStar.DBZ },
            { "White Dwarf (DBV) Star", EDStar.DBV },
            { "White Dwarf (DQ) Star", EDStar.DQ },
            { "White Dwarf (DC) Star", EDStar.DC },
            { "White Dwarf (DCV) Star", EDStar.DCV },
            { "Neutron Star", EDStar.N },
            { "Black Hole", EDStar.H },
            { "A (Blue-White super giant) Star", EDStar.A_BlueWhiteSuperGiant },
            { "F (White super giant) Star", EDStar.F_WhiteSuperGiant },
            { "M (Red super giant) Star", EDStar.M_RedSuperGiant },
            { "M (Red giant) Star", EDStar.M_RedGiant},
            { "K (Yellow-Orange giant) Star", EDStar.K_OrangeGiant },
            { "Supermassive Black Hole", EDStar.SuperMassiveBlackHole },
            { "B (Blue-White super giant) Star", EDStar.B_BlueWhiteSuperGiant },
            { "G (White-Yellow super giant) Star", EDStar.G_WhiteSuperGiant },
        };

        private readonly Dictionary<(string, int), Sector> Sectors = new();
        
        private readonly Dictionary<int, Sector> SectorList = new();

        private readonly Dictionary<string, int> SectorNames = new(StringComparer.OrdinalIgnoreCase);

        private readonly Dictionary<long, string> Names = new();

        private readonly Dictionary<long, string> OrigNames = new();

        private readonly Dictionary<long, SystemEntry> Systems;

        private readonly int EstimatedSystemsCount;

        private readonly HashSet<long> PermitSystems = new();

        private static readonly Dictionary<string, string> Selectors = new()
        {
            ["All"] = "All",
            ["Bubble"] = "810",
            ["ExtendedBubble"] = "608,609,610,611,612,708,709,710,711,712,808,809,810,811,812,908,909,910,911,912,1008,1009,1010,1011,1012",
            ["BubbleColonia"] = "608,609,610,611,612,708,709,710,711,712,808,809,810,811,812,908,909,910,911,912,1008,1009,1010,1011,1012,1108,1109,1110,1207,1208,1209,1306,1307,1308,1405,1406,1407,1504,1505,1603,1604,1703"
        };

        public SystemsDB(int estsyscount)
        {
            Systems = new(estsyscount);
            EstimatedSystemsCount = estsyscount;
        }

        private SqliteConnection CreateConnection(string selname)
        {
            var csb = new SqliteConnectionStringBuilder
            {
                DataSource = Path.Combine(BasePath, $"EDDSystem-{selname}.sqlite")
            };

            return new SqliteConnection(csb.ToString());
        }
        
        public void Init()
        {
            foreach (string selname in Selectors.Keys)
            {
                Init(selname);
            }

            LoadExisting();
        }

        private void Init(string selname)
        {
            using var conn = CreateConnection(selname);
            conn.Open();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS Register (" +
                        "ID TEXT PRIMARY KEY NOT NULL, " +
                        "ValueInt INTEGER, " +
                        "ValueDouble DOUBLE, " +
                        "ValueString TEXT, " +
                        "ValueBlob BLOB" +
                    ")";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS Sectors (" +
                        "id INTEGER PRIMARY KEY NOT NULL, " +
                        "gridid INTEGER, " +
                        "Name TEXT NOT NULL COLLATE NOCASE" +
                    ")";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS SystemTable (" +
                        "edsmid INTEGER PRIMARY KEY NOT NULL , " +
                        "sectorid INTEGER, " +
                        "nameid INTEGER, " +
                        "x INTEGER, " +
                        "y INTEGER, " +
                        "z INTEGER, " +
                        "info INTEGER" +
                    ")";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS Names (" +
                        "id INTEGER PRIMARY KEY NOT NULL , " +
                        "Name TEXT NOT NULL COLLATE NOCASE " +
                    ")";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS Aliases (" +
                        "edsmid INTEGER PRIMARY KEY NOT NULL, " +
                        "edsmid_mergedto INTEGER, " +
                        "name TEXT COLLATE NOCASE" +
                    ")";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS PermitSystems (" +
                        "edsmid INTEGER PRIMARY KEY NOT NULL" +
                    ")";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT OR IGNORE INTO Register (ID, ValueInt) VALUES ('DBVer', 213)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT OR IGNORE INTO Register (ID, ValueString) VALUES ('DBSource', 'SPANSH')";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT OR IGNORE INTO Register (ID, ValueString) VALUES ('EDSMGridIDs', @GridIds)";
                cmd.Parameters.AddWithValue("@GridIds", Selectors.TryGetValue(selname, out var gridids) ? gridids : "All");
                cmd.ExecuteNonQuery();
            }
        }

        private void LoadExisting()
        {
            using var conn = CreateConnection("All");
            conn.Open();

            Console.Error.WriteLine("Loading sectors");
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT Id, Name, GridId FROM Sectors";
                using var rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    var sector = new Sector
                    {
                        Id = rdr.GetInt32(0),
                        Name = rdr.GetString(1),
                        GridId = rdr.GetInt32(2)
                    };

                    Sectors[(sector.Name.ToLowerInvariant(), sector.GridId)] = sector;
                    SectorList[sector.Id] = sector;

                    if (sector.Id < 0)
                    {
                        var nameid = -sector.Id / 10000;
                        var name = sector.Name;

                        if (!SectorNames.ContainsKey(name))
                        {
                            SectorNames[name] = nameid;
                        }
                    }
                }
            }

            Console.Error.WriteLine("Loading names");
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT Id, Name FROM Names";
                using var rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    var id = rdr.GetInt64(0);
                    Names[id] = rdr.GetString(1);
                    OrigNames[id] = Names[id];
                }
            }

            int num = 0;

            Console.Error.WriteLine("Loading systems");
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT EdsmId, SectorId, NameId, X, Y, Z, Info FROM SystemTable";

                using var rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    var system = new SystemEntry
                    {
                        SystemAddress = rdr.GetInt64(0),
                        SectorId = rdr.GetInt32(1),
                        NameId = rdr.GetInt64(2),
                        X = rdr.GetInt32(3),
                        Y = rdr.GetInt32(4),
                        Z = rdr.GetInt32(5),
                        Info = rdr.IsDBNull(6) ? 0 : rdr.GetInt32(6)
                    };

                    Systems[system.SystemAddress] = system;

                    num++;

                    if ((num % 1000) == 0)
                    {
                        Console.Error.Write(".");

                        if ((num % 64000) == 0)
                        {
                            Console.Error.WriteLine($" {num}");
                        }

                        Console.Error.Flush();
                    }
                }
            }

            Console.Error.WriteLine("Loading permit systems");
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT edsmid FROM PermitSystems";

                using var rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    PermitSystems.Add(rdr.GetInt64(0));
                }
            }

            Console.Error.Write($" {num}\n");
        }

        private bool SplitProcgenName(string systemname, out string regionname, out int mid, out int n2, out int masscode)
        {
            if (string.IsNullOrWhiteSpace(systemname)) goto fail;

            var sn = systemname;

            int i = sn.Length - 1;

            if (i < 9) goto fail;                                   // a bc-d e0

            if (sn[i] < '0' || sn[i] > '9') goto fail;              // cepheus dark region a sector xy-z a1-[0]

            n2 = 0;
            int mult = 1;
            while (i > 8 && sn[i] >= '0' && sn[i] <= '9')
            {
                n2 += (sn[i] - '0') * mult;
                i--;
                mult *= 10;
            }

            mid = 0;
            if (sn[i] == '-')                                          // cepheus dark region a sector xy-z a1[-]0
            {
                i--;

                int vend = i;
                mult = 1;
                while (i > 8 && sn[i] >= '0' && sn[i] <= '9')          // cepheus dark region a sector xy-z a[1]-0
                {
                    mid += (sn[i] - '0') * mult;
                    i--;
                    mult *= 10;
                }

                if (i == vend) goto fail;
            }

            mid *= 26 * 26 * 26;

            if (sn[i] < 'a' || sn[i] > 'h') goto fail;              // cepheus dark region a sector xy-z [a]1-0
            masscode = (sn[i] - 'a');
            i--;
            if (sn[i] != ' ') goto fail;                            // cepheus dark region a sector xy-z[ ]a1-0
            i--;
            if (sn[i] < 'A' || sn[i] > 'Z') goto fail;              // cepheus dark region a sector xy-[z] a1-0
            mid += (sn[i] - 'A') * 26 * 26;
            i--;
            if (sn[i] != '-') goto fail;                            // cepheus dark region a sector xy[-]z a1-0
            i--;
            if (sn[i] < 'A' || sn[i] > 'Z') goto fail;              // cepheus dark region a sector x[y]-z a1-0
            mid += (sn[i] - 'A') * 26;
            i--;
            if (sn[i] < 'A' || sn[i] > 'Z') goto fail;              // cepheus dark region a sector [x]y-z a1-0
            mid += (sn[i] - 'A');
            i--;
            if (sn[i] != ' ') goto fail;                            // cepheus dark region a sector[ ]xy-z a1-0
            regionname = systemname.Substring(0, i);                // [cepheus dark region a sector] xy-z a1-0
            return true;

            fail:
            regionname = null;
            mid = 0;
            n2 = 0;
            masscode = 0;
            return false;
        }

        private long GetNameId(string name, long systemAddress)
        {
            Names[systemAddress] = name;
            return systemAddress;
        }

        private void ProcessSystemName(ref SystemEntry system, string name)
        {
            string sectorname = null;

            if (SplitProcgenName(name, out var regionname, out int mid, out int n2, out int masscode))
            {
                sectorname = regionname;
                var l1 = mid % 26;
                var l2 = (mid / 26) % 26;
                var l3 = (mid / 26 / 26) % 26;
                var n1 = mid / 26 / 26 / 26;
                system = system with { NameId = n2 + (n1 << 16) + (masscode << 24) + ((long)(l3 + 1) << 28) + ((long)(l2 + 1) << 33) + ((long)(l1 + 1) << 38) + (1L << 47) };
            }
            else
            {
                var nameparts = name.Split(' ');

                if (nameparts.Length >= 2)
                {
                    var dashpos = nameparts[^1].IndexOf('-');
                    var num = dashpos >= 0 && nameparts[^1].Count(e => e == '-') == 1 ? nameparts[^1].Replace("-", "") : nameparts[^1];

                    if (long.TryParse(num, out var v) && v < 0x3fffffffff)
                    {
                        sectorname = string.Join(" ", nameparts[..^1]);
                        system = system with { NameId = v + (1L << 46) + ((long)(dashpos + 1) << 38) + ((long)num.Length << 42) };
                    }
                    else if (Surveys.Contains(nameparts[0]))
                    {
                        sectorname = nameparts[0];
                        system = system with { NameId = GetNameId(string.Join(" ", nameparts[1..]), system.SystemAddress) };
                    }
                    else
                    {
                        sectorname = "NoSectorName";
                        system = system with { NameId = GetNameId(name, system.SystemAddress) };
                    }
                }
                else
                {
                    sectorname = "NoSectorName";
                    system = system with { NameId = GetNameId(name, system.SystemAddress) };
                }
            }

            var gx = Array.BinarySearch(GridX, (int)Math.Floor((system.X / 128 + 19500) / 1000.0)) + 1;
            var gz = Array.BinarySearch(GridZ, (int)Math.Floor((system.Z / 128 + 9500) / 1000.0)) + 1;

            if (gx < 0) gx = -gx;
            if (gz < 0) gz = -gz;
            var gridid = gz * 100 + gx;

            if (!Sectors.TryGetValue((sectorname.ToLowerInvariant(), gridid), out var sector))
            {
                var id = SectorList.Keys.OrderByDescending(e => e).FirstOrDefault() + 1;

                if (id <= 0)
                {
                    id = 1;
                }

                sector = new Sector { Id = id, Name = sectorname, GridId = gridid };
                SectorList[id] = sector;
                Sectors[(sectorname.ToLowerInvariant(), gridid)] = sector;
            }

            system = system with { SectorId = sector.Id };
        }

        private (int x, int y, int z) GetCoords(ref Utf8JsonReader jrdr)
        {
            int x = int.MinValue;
            int y = int.MinValue;
            int z = int.MinValue;

            while (jrdr.Read() && jrdr.TokenType != JsonTokenType.EndObject)
            {
                var name = jrdr.GetString();
                jrdr.Read();

                switch ((name, jrdr.TokenType))
                {
                    case ("x", JsonTokenType.Number): x = (int)Math.Floor(jrdr.GetDouble() * 128 + 0.5); break;
                    case ("y", JsonTokenType.Number): y = (int)Math.Floor(jrdr.GetDouble() * 128 + 0.5); break;
                    case ("z", JsonTokenType.Number): z = (int)Math.Floor(jrdr.GetDouble() * 128 + 0.5); break;
                    default: throw new InvalidOperationException();
                }
            }

            return (x, y, z);
        }

        public void LoadNames(string csvpath)
        {
            foreach (var line in File.ReadAllLines(csvpath).Skip(1))
            {
                var fields = line.Split(',', 2);
                SectorNames[fields[1]] = int.Parse(fields[0]);
            }
        }

        public void SaveNames(string csvpath)
        {
            using (var stream = File.Open(csvpath + ".tmp", FileMode.Create, FileAccess.Write, FileShare.Read))
            {
                using var writer = new StreamWriter(stream);
                writer.WriteLine("ID,Name");
                foreach (var kvp in SectorNames.OrderBy(e => e.Key))
                {
                    writer.WriteLine($"{kvp.Value},{kvp.Key}");
                }
            }

            File.Move(csvpath + ".tmp", csvpath, true);
        }

        public void LoadSystems(string gzpath)
        {
            using var stream = new GZipInputStream(File.Open(gzpath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
            var buffer = new byte[65536];
            var span = new Memory<byte>(buffer);
            int pos = 0;
            int end = 0;
            bool eof = false;
            var addsystems = new List<SystemEntry>(EstimatedSystemsCount - Systems.Count);
            var updsystems = new List<SystemEntry>();
            var addPermits = new HashSet<long>();
            var delPermits = new HashSet<long>();
            var lastsectorid = SectorList.Keys.OrderByDescending(e => e).FirstOrDefault();
            var nameids = Names.Keys.ToHashSet();
            int num = 0;
            var lastdate = DateTime.MinValue;

            end = stream.Read(span.Span);

            while (!eof || pos < end)
            {
                var line = span[pos..end];
                var endlpos = line.Span.IndexOf((byte)'\n');

                if (endlpos < 0 && pos != 0 && !eof)
                {
                    line.CopyTo(span);
                    end -= pos;
                    pos = 0;
                    var len = stream.Read(span.Span[end..]);

                    if (len == 0) eof = true;

                    end += len;
                    line = span[pos..end];
                    endlpos = line.Span.IndexOf((byte)'\n');
                }

                if (pos == 0 && end == 0 && eof)
                {
                    break;
                }

                if (endlpos < 0)
                {
                    if (end == buffer.Length)
                    {
                        throw new NotSupportedException("Line too long");
                    }
                    else
                    {
                        endlpos = end;
                    }
                }

                line = line[..endlpos];

                if (line.Length > 4)
                {
                    var json = Encoding.UTF8.GetString(line.Span);

                    var jrdr = new Utf8JsonReader(line.Span, new JsonReaderOptions { AllowTrailingCommas = true });

                    jrdr.Read();

                    if (jrdr.TokenType != JsonTokenType.StartObject)
                    {
                        throw new InvalidOperationException();
                    }

                    string sysname = null;
                    string mainStar = null;
                    long sysaddr = 0;
                    int x = 0;
                    int y = 0;
                    int z = 0;
                    bool permit = false;
                    DateTime date = default;

                    while (jrdr.Read() && jrdr.TokenType != JsonTokenType.EndObject)
                    {
                        var name = jrdr.GetString();
                        jrdr.Read();

                        switch ((name, jrdr.TokenType))
                        {
                            case ("id64", JsonTokenType.Number): sysaddr = jrdr.GetInt64(); break;
                            case ("name", JsonTokenType.String): sysname = jrdr.GetString(); break;
                            case ("coords", JsonTokenType.StartObject): (x, y, z) = GetCoords(ref jrdr); break;
                            case ("date", JsonTokenType.String): date = DateTime.Parse(jrdr.GetString()); break;
                            case ("updateTime", JsonTokenType.String): date = DateTime.Parse(jrdr.GetString()); break;
                            case ("needsPermit", JsonTokenType.String): permit = jrdr.GetBoolean(); break;
                            case ("mainStar", JsonTokenType.String): mainStar = jrdr.GetString(); break;
                        }
                    }

                    var system = new SystemEntry
                    {
                        SystemAddress = sysaddr,
                        X = x,
                        Y = y,
                        Z = z,
                        Info = mainStar != null && SpanshToEDStar.TryGetValue(mainStar, out var edstar) ? (int)edstar : 0
                    };

                    if (permit && !PermitSystems.Contains(sysaddr))
                    {
                        addPermits.Add(sysaddr);
                    }
                    else if (!permit && PermitSystems.Contains(sysaddr))
                    {
                        delPermits.Add(sysaddr);
                    }

                    if (date > lastdate)
                    {
                        lastdate = date;
                    }

                    ProcessSystemName(ref system, sysname);

                    if (!Systems.TryGetValue(system.SystemAddress, out var oldsys))
                    {
                        Systems[system.SystemAddress] = system;
                        addsystems.Add(system);
                    }
                    else if (system != oldsys)
                    {
                        Systems[system.SystemAddress] = system;
                        updsystems.Add(system);
                    }
                }

                pos += endlpos + 1;
                num++;

                if ((num % 1000) == 0)
                {
                    Console.Error.Write(".");

                    if ((num % 64000) == 0)
                    {
                        Console.Error.Write($" {num}\n");
                    }

                    Console.Error.Flush();
                }
            }

            Console.Error.WriteLine($" {num}");

            Console.Error.WriteLine("Sorting new sectors");

            var newsectors =
                SectorList
                    .Where(e => e.Key > 0)
                    .OrderBy(e => e.Value.Name, StringComparer.OrdinalIgnoreCase)
                    .ToList();

            var sectormap = new Dictionary<int, int>();

            foreach (var kvp in newsectors)
            {
                var sector = kvp.Value;
                SectorList.Remove(kvp.Key);
                
                if (!SectorNames.TryGetValue(sector.Name, out var nameid))
                {
                    nameid = SectorNames.Values.OrderByDescending(e => e).FirstOrDefault() + 1;
                    SectorNames[sector.Name] = nameid;
                }

                sector.Id = -(nameid * 10000 + sector.GridId);
                sectormap[kvp.Key] = sector.Id;
                SectorList[sector.Id] = sector;
            }

            for (int i = 0; i < addsystems.Count; i++)
            {
                var system = addsystems[i];
                if (sectormap.TryGetValue(system.SectorId, out var newsectorid))
                {
                    system = system with { SectorId = newsectorid };
                    addsystems[i] = system;
                    Systems[system.SystemAddress] = system;
                }
            }

            for (int i = 0; i < updsystems.Count; i++)
            {
                var system = updsystems[i];
                if (sectormap.TryGetValue(system.SectorId, out var newsectorid))
                {
                    system = system with { SectorId = newsectorid };
                    updsystems[i] = system;
                    Systems[system.SystemAddress] = system;
                }
            }

            var updSectorIds = new List<SystemEntry>();

            foreach (var system in Systems.Values)
            {
                if (sectormap.TryGetValue(system.SectorId, out var newsectorid) && newsectorid != system.SectorId)
                {
                    var updsystem = system with { SectorId = newsectorid };
                    updSectorIds.Add(updsystem);
                }
            }

            foreach (var system in updSectorIds)
            {
                Systems[system.SystemAddress] = system;
            }

            foreach (var selkvp in Selectors)
            {
                Console.Error.WriteLine($"Saving database for selection {selkvp.Key}");

                HashSet<int> gridids;

                var grididlist = selkvp.Value.Split(',');

                if (grididlist.All(e => int.TryParse(e, out _)))
                {
                    gridids = grididlist.Select(e => int.Parse(e)).ToHashSet();
                }
                else
                {
                    gridids = SectorList.Values.Select(e => e.GridId).Distinct().ToHashSet();
                }

                var sysids = Systems.Values.Where(e => SectorList.TryGetValue(e.SectorId, out var sector) && gridids.Contains(sector.GridId)).Select(e => e.SystemAddress).ToHashSet();

                Console.Error.WriteLine("Saving sectors");
                using var conn = CreateConnection(selkvp.Key);
                conn.Open();
                using var txn = conn.BeginTransaction();

                var currentSectors = new HashSet<int>();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "SELECT Id FROM Sectors";
                    using var rdr = cmd.ExecuteReader();

                    while (rdr.Read())
                    {
                        currentSectors.Add(rdr.GetInt32(0));
                    }
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT INTO Sectors (Id, Name, GridId) VALUES (@Id, @Name, @GridId)";
                    var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);
                    var nameparam = cmd.Parameters.Add("@Name", SqliteType.Text);
                    var grididparam = cmd.Parameters.Add("@GridId", SqliteType.Integer);

                    foreach (var sector in SectorList.Values)
                    {
                        if (!currentSectors.Contains(sector.Id) && gridids.Contains(sector.GridId))
                        {
                            idparam.Value = sector.Id;
                            nameparam.Value = sector.Name;
                            grididparam.Value = sector.GridId;
                            cmd.ExecuteNonQuery();
                            currentSectors.Add(sector.Id);
                        }
                    }
                }

                Console.Error.WriteLine("Saving names");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT OR REPLACE INTO Names (Id, Name) VALUES (@Id, @Name)";
                    var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);
                    var nameparam = cmd.Parameters.Add("@Name", SqliteType.Text);

                    foreach (var kvp in Names)
                    {
                        var id = kvp.Key;
                        var name = kvp.Value;

                        if (sysids.Contains(id) && (!OrigNames.TryGetValue(id, out var origname) || origname != name))
                        {
                            idparam.Value = id;
                            nameparam.Value = name;
                            cmd.ExecuteNonQuery();
                        }
                    }
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "DELETE FROM Names WHERE Id = @Id";
                    var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);

                    foreach (var kvp in Names)
                    {
                        var id = kvp.Key;

                        if (!sysids.Contains(id))
                        {
                            idparam.Value = id;
                            cmd.ExecuteNonQuery();
                        }
                    }
                }

                using (var cmd = conn.CreateCommand())
                {
                    var id = SectorList.Keys.OrderByDescending(e => e).FirstOrDefault() + 1;

                    if (id <= 0)
                    {
                        id = 1;
                    }

                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT OR REPLACE INTO Register (Id, ValueInt) VALUES (@Id, @Val)";
                    cmd.Parameters.AddWithValue("@Id", "EDSMSectorIDNext");
                    cmd.Parameters.AddWithValue("@Val", id);
                    cmd.ExecuteNonQuery();
                }

                Console.Error.WriteLine("Adding systems");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT OR REPLACE INTO SystemTable (EdsmId, SectorId, NameId, X, Y, Z, Info) VALUES (@Id, @SectorId, @NameId, @X, @Y, @Z, @Info)";
                    var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);
                    var sectoridparam = cmd.Parameters.Add("@SectorId", SqliteType.Integer);
                    var nameidparam = cmd.Parameters.Add("@NameId", SqliteType.Integer);
                    var xparam = cmd.Parameters.Add("@X", SqliteType.Integer);
                    var yparam = cmd.Parameters.Add("@Y", SqliteType.Integer);
                    var zparam = cmd.Parameters.Add("@Z", SqliteType.Integer);
                    var infoparam = cmd.Parameters.Add("@Info", SqliteType.Integer);

                    num = 0;
                    foreach (var system in addsystems)
                    {
                        if (sysids.Contains(system.SystemAddress))
                        {
                            idparam.Value = system.SystemAddress;
                            sectoridparam.Value = system.SectorId;
                            nameidparam.Value = system.NameId;
                            xparam.Value = system.X;
                            yparam.Value = system.Y;
                            zparam.Value = system.Z;
                            infoparam.Value = system.Info == 0 ? DBNull.Value : system.Info;
                            cmd.ExecuteNonQuery();

                            num++;

                            if ((num % 1000) == 0)
                            {
                                Console.Error.Write(".");

                                if ((num % 64000) == 0)
                                {
                                    Console.Error.WriteLine($" {num}");
                                }

                                Console.Error.Flush();
                            }
                        }
                    }
                }

                Console.Error.WriteLine($" {num}");
                Console.Error.WriteLine("Updating systems");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT OR REPLACE INTO SystemTable (EdsmId, SectorId, NameId, X, Y, Z, Info) VALUES (@Id, @SectorId, @NameId, @X, @Y, @Z, @Info)";
                    var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);
                    var sectoridparam = cmd.Parameters.Add("@SectorId", SqliteType.Integer);
                    var nameidparam = cmd.Parameters.Add("@NameId", SqliteType.Integer);
                    var xparam = cmd.Parameters.Add("@X", SqliteType.Integer);
                    var yparam = cmd.Parameters.Add("@Y", SqliteType.Integer);
                    var zparam = cmd.Parameters.Add("@Z", SqliteType.Integer);
                    var infoparam = cmd.Parameters.Add("@Info", SqliteType.Integer);

                    num = 0;

                    foreach (var system in updsystems)
                    {
                        if (sysids.Contains(system.SystemAddress))
                        {
                            idparam.Value = system.SystemAddress;
                            sectoridparam.Value = system.SectorId;
                            nameidparam.Value = system.NameId;
                            xparam.Value = system.X;
                            yparam.Value = system.Y;
                            zparam.Value = system.Z;
                            infoparam.Value = system.Info == 0 ? DBNull.Value : system.Info;
                            cmd.ExecuteNonQuery();

                            num++;

                            if ((num % 1000) == 0)
                            {
                                Console.Error.Write(".");

                                if ((num % 64000) == 0)
                                {
                                    Console.Error.WriteLine($" {num}");
                                }

                                Console.Error.Flush();
                            }
                        }
                    }
                }

                Console.Error.WriteLine($" {num}");
                Console.Error.WriteLine("Deleting systems");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "DELETE FROM SystemTable WHERE EdsmId = @Id";
                    var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);

                    num = 0;

                    foreach (var system in updsystems)
                    {
                        if (!sysids.Contains(system.SystemAddress))
                        {
                            idparam.Value = system.SystemAddress;
                            cmd.ExecuteNonQuery();

                            num++;

                            if ((num % 1000) == 0)
                            {
                                Console.Error.Write(".");

                                if ((num % 64000) == 0)
                                {
                                    Console.Error.WriteLine($" {num}");
                                }

                                Console.Error.Flush();
                            }
                        }
                    }
                }

                Console.Error.WriteLine($" {num}");
                Console.Error.WriteLine("Adding new permit systems");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT OR REPLACE INTO PermitSystems (edsmid) VALUES (@Id)";
                    var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);

                    foreach (var sysaddr in addPermits)
                    {
                        idparam.Value = sysaddr;
                        cmd.ExecuteNonQuery();
                    }
                }

                Console.Error.WriteLine("Removing deleted permit systems");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "DELETE FROM PermitSystems WHERE edsmid = @Id";
                    var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);

                    foreach (var sysaddr in delPermits)
                    {
                        idparam.Value = sysaddr;
                        cmd.ExecuteNonQuery();
                    }
                }

                Console.Error.WriteLine("Creating indexes");

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS SystemsSectorName ON SystemTable (sectorId,nameid)";
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS SystemsXZY ON SystemTable (x,z,y)";
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS NamesName ON Names (Name)";
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS SectorName ON Sectors (name)";
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS SectorGridId ON Sectors (gridid)";
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT OR REPLACE INTO Register (Id, ValueString) VALUES (@Id, @Val)";
                    cmd.Parameters.AddWithValue("@Id", "EDSMLastSystems");
                    cmd.Parameters.AddWithValue("@Val", lastdate.ToString("s"));
                    cmd.ExecuteNonQuery();
                }

                Console.Error.WriteLine("Committing");
                txn.Commit();
            }
        }
    }
}
