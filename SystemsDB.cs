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

        private readonly HashSet<string> Surveys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
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

        private readonly int[] GridZ = new[]
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

        private readonly int[] GridX = new[]
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

        private readonly Dictionary<(string, int), Sector> Sectors = new Dictionary<(string, int), Sector>();
        
        private readonly Dictionary<int, Sector> SectorList = new Dictionary<int, Sector>();

        private readonly Dictionary<string, int> SectorNames = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        private readonly HashSet<int> CurrentSectors = new HashSet<int>();

        private readonly Dictionary<int, string> Names = new Dictionary<int, string>();

        private readonly Dictionary<int, string> OrigNames = new Dictionary<int, string>();

        private readonly List<SystemEntry> Systems = new List<SystemEntry>();

        private readonly Dictionary<string, string> Selectors = new Dictionary<string, string>
        {
            ["All"] = "All",
            ["Bubble"] = "810",
            ["ExtendedBubble"] = "608,609,610,611,612,708,709,710,711,712,808,809,810,811,812,908,909,910,911,912,1008,1009,1010,1011,1012",
            ["BubbleColonia"] = "608,609,610,611,612,708,709,710,711,712,808,809,810,811,812,908,909,910,911,912,1008,1009,1010,1011,1012,1108,1109,1110,1207,1208,1209,1306,1307,1308,1405,1406,1407,1504,1505,1603,1604,1703"
        };

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
                    "CREATE TABLE IF NOT EXISTS Systems (" +
                        "edsmid INTEGER PRIMARY KEY NOT NULL , " +
                        "sectorid INTEGER, " +
                        "nameid INTEGER, " +
                        "x INTEGER, " +
                        "y INTEGER, " +
                        "z INTEGER" +
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
                cmd.CommandText = "INSERT OR IGNORE INTO Register (ID, ValueInt) VALUES ('DBVer', 200)";
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
                    CurrentSectors.Add(sector.Id);

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
                    var id = rdr.GetInt32(0);
                    Names[id] = rdr.GetString(1);
                    OrigNames[id] = Names[id];
                }
            }

            int num = 0;

            Console.Error.WriteLine("Loading systems");
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT EdsmId, SectorId, NameId, X, Y, Z FROM Systems";

                using var rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    var system = new SystemEntry
                    {
                        EdsmId = rdr.GetInt32(0),
                        SectorId = rdr.GetInt32(1),
                        NameId = rdr.GetInt64(2),
                        X = rdr.GetInt32(3),
                        Y = rdr.GetInt32(4),
                        Z = rdr.GetInt32(5)
                    };

                    while (Systems.Count < system.EdsmId)
                    {
                        Systems.Add(default);
                    }

                    if (Systems.Count == system.EdsmId)
                    {
                        Systems.Add(system);
                    }
                    else
                    {
                        Systems[system.EdsmId] = system;
                    }

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

        private int GetNameId(string name, int edsmid)
        {
            Names[edsmid] = name;
            return edsmid;
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
                system.NameId = n2 + (n1 << 16) + (masscode << 24) + ((long)(l3 + 1) << 28) + ((long)(l2 + 1) << 33) + ((long)(l1 + 1) << 38) + (1L << 47);
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
                        system.NameId = v + (1L << 46) + ((long)(dashpos + 1) << 38) + ((long)num.Length << 42);
                    }
                    else if (Surveys.Contains(nameparts[0]))
                    {
                        sectorname = nameparts[0];
                        system.NameId = GetNameId(string.Join(" ", nameparts[1..]), system.EdsmId);
                    }
                    else
                    {
                        sectorname = "NoSectorName";
                        system.NameId = GetNameId(name, system.EdsmId);
                    }
                }
                else
                {
                    sectorname = "NoSectorName";
                    system.NameId = GetNameId(name, system.EdsmId);
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

            system.SectorId = sector.Id;
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
                foreach (var kvp in SectorNames)
                {
                    writer.WriteLine($"{kvp.Value},{kvp.Key}");
                }
            }

            File.Move(csvpath + ".tmp", csvpath, true);
        }

        public void DownloadSystems(string gzpath)
        {
            var req = WebRequest.CreateHttp("https://www.edsm.net/dump/systemsWithCoordinates.json.gz");
            using var resp = req.GetResponse();
            using var respstream = resp.GetResponseStream();
            using var stream = File.Open(gzpath, FileMode.Create, FileAccess.Write, FileShare.Read);
            respstream.CopyTo(stream);
        }

        public void LoadSystems(string gzpath)
        {
            using var stream = new GZipInputStream(File.Open(gzpath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
            var buffer = new byte[65536];
            var span = new Memory<byte>(buffer);
            int pos = 0;
            int end = 0;
            bool eof = false;
            var addsystems = new List<SystemEntry>();
            var updsystems = new List<SystemEntry>();
            var lastsectorid = SectorList.Keys.OrderByDescending(e => e).FirstOrDefault();
            var nameids = Names.Keys.ToHashSet();
            var lastedsmid = Systems.Count;
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

                    var system = new SystemEntry();
                    string sysname = null;
                    DateTime date = default;

                    while (jrdr.Read() && jrdr.TokenType != JsonTokenType.EndObject)
                    {
                        var name = jrdr.GetString();
                        jrdr.Read();

                        switch ((name, jrdr.TokenType))
                        {
                            case ("id", JsonTokenType.Number): system.EdsmId = jrdr.GetInt32(); break;
                            case ("id64", JsonTokenType.Number): system.SystemAddress = jrdr.GetInt64(); break;
                            case ("name", JsonTokenType.String): sysname = jrdr.GetString(); break;
                            case ("coords", JsonTokenType.StartObject): (system.X, system.Y, system.Z) = GetCoords(ref jrdr); break;
                            case ("date", JsonTokenType.String): date = DateTime.Parse(jrdr.GetString()); break;
                        }
                    }

                    if (date > lastdate)
                    {
                        lastdate = date;
                    }

                    ProcessSystemName(ref system, sysname);

                    while (Systems.Count < system.EdsmId)
                    {
                        Systems.Add(default);
                    }

                    if (Systems.Count == system.EdsmId)
                    {
                        Systems.Add(system);
                    }
                    else if (Systems[system.EdsmId].EdsmId == 0)
                    {
                        Systems[system.EdsmId] = system;

                        if (system.EdsmId <= lastedsmid)
                        {
                            addsystems.Add(system);
                        }
                    }
                    else
                    {
                        var oldsys = Systems[system.EdsmId];
                        if (system != oldsys)
                        {
                            Systems[system.EdsmId] = system;
                            updsystems.Add(system);
                        }
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
                    system.SectorId = newsectorid;
                    addsystems[i] = system;
                }
            }

            for (int i = 0; i < updsystems.Count; i++)
            {
                var system = updsystems[i];
                if (sectormap.TryGetValue(system.SectorId, out var newsectorid))
                {
                    system.SectorId = newsectorid;
                    updsystems[i] = system;
                }
            }

            for (int i = lastedsmid + 1; i < Systems.Count; i++)
            {
                var system = Systems[i];
                if (system.EdsmId == i && sectormap.TryGetValue(system.SectorId, out var newsectorid))
                {
                    system.SectorId = newsectorid;
                    Systems[i] = system;
                }
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

                var sysids = Systems.Where(e => SectorList.TryGetValue(e.SectorId, out var sector) && gridids.Contains(sector.GridId)).Select(e => e.EdsmId).ToHashSet();

                Console.Error.WriteLine("Saving sectors");
                using var conn = CreateConnection(selkvp.Key);
                conn.Open();
                using var txn = conn.BeginTransaction();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT INTO Sectors (Id, Name, GridId) VALUES (@Id, @Name, @GridId)";
                    var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);
                    var nameparam = cmd.Parameters.Add("@Name", SqliteType.Text);
                    var grididparam = cmd.Parameters.Add("@GridId", SqliteType.Integer);

                    foreach (var sector in SectorList.Values)
                    {
                        if (!CurrentSectors.Contains(sector.Id) && gridids.Contains(sector.GridId))
                        {
                            idparam.Value = sector.Id;
                            nameparam.Value = sector.Name;
                            grididparam.Value = sector.GridId;
                            cmd.ExecuteNonQuery();
                            CurrentSectors.Add(sector.Id);
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

                Console.Error.WriteLine("Saving inserted systems");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT OR REPLACE INTO Systems (EdsmId, SectorId, NameId, X, Y, Z) VALUES (@EdsmId, @SectorId, @NameId, @X, @Y, @Z)";
                    var edsmidparam = cmd.Parameters.Add("@EdsmId", SqliteType.Integer);
                    var sectoridparam = cmd.Parameters.Add("@SectorId", SqliteType.Integer);
                    var nameidparam = cmd.Parameters.Add("@NameId", SqliteType.Integer);
                    var xparam = cmd.Parameters.Add("@X", SqliteType.Integer);
                    var yparam = cmd.Parameters.Add("@Y", SqliteType.Integer);
                    var zparam = cmd.Parameters.Add("@Z", SqliteType.Integer);

                    num = 0;
                    foreach (var system in addsystems)
                    {
                        if (sysids.Contains(system.EdsmId))
                        {
                            edsmidparam.Value = system.EdsmId;
                            sectoridparam.Value = system.SectorId;
                            nameidparam.Value = system.NameId;
                            xparam.Value = system.X;
                            yparam.Value = system.Y;
                            zparam.Value = system.Z;
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
                    cmd.CommandText = "INSERT OR REPLACE INTO Systems (EdsmId, SectorId, NameId, X, Y, Z) VALUES (@EdsmId, @SectorId, @NameId, @X, @Y, @Z)";
                    var edsmidparam = cmd.Parameters.Add("@EdsmId", SqliteType.Integer);
                    var sectoridparam = cmd.Parameters.Add("@SectorId", SqliteType.Integer);
                    var nameidparam = cmd.Parameters.Add("@NameId", SqliteType.Integer);
                    var xparam = cmd.Parameters.Add("@X", SqliteType.Integer);
                    var yparam = cmd.Parameters.Add("@Y", SqliteType.Integer);
                    var zparam = cmd.Parameters.Add("@Z", SqliteType.Integer);

                    num = 0;

                    foreach (var system in updsystems)
                    {
                        if (sysids.Contains(system.EdsmId))
                        {
                            edsmidparam.Value = system.EdsmId;
                            sectoridparam.Value = system.SectorId;
                            nameidparam.Value = system.NameId;
                            xparam.Value = system.X;
                            yparam.Value = system.Y;
                            zparam.Value = system.Z;
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

                Console.WriteLine($" {num}");
                Console.Error.WriteLine("Deleting systems");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "DELETE FROM Systems WHERE EdsmId = @EdsmId";
                    var edsmidparam = cmd.Parameters.Add("@EdsmId", SqliteType.Integer);

                    num = 0;

                    foreach (var system in updsystems)
                    {
                        if (!sysids.Contains(system.EdsmId))
                        {
                            edsmidparam.Value = system.EdsmId;
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

                Console.WriteLine($" {num}");
                Console.Error.WriteLine("Appending systems");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT OR REPLACE INTO Systems (EdsmId, SectorId, NameId, X, Y, Z) VALUES (@EdsmId, @SectorId, @NameId, @X, @Y, @Z)";
                    var edsmidparam = cmd.Parameters.Add("@EdsmId", SqliteType.Integer);
                    var sectoridparam = cmd.Parameters.Add("@SectorId", SqliteType.Integer);
                    var nameidparam = cmd.Parameters.Add("@NameId", SqliteType.Integer);
                    var xparam = cmd.Parameters.Add("@X", SqliteType.Integer);
                    var yparam = cmd.Parameters.Add("@Y", SqliteType.Integer);
                    var zparam = cmd.Parameters.Add("@Z", SqliteType.Integer);

                    num = 0;

                    for (int i = lastedsmid; i < Systems.Count; i++)
                    {
                        var system = Systems[i];

                        if (system != default && sysids.Contains(i))
                        {
                            edsmidparam.Value = system.EdsmId;
                            sectoridparam.Value = system.SectorId;
                            nameidparam.Value = system.NameId;
                            xparam.Value = system.X;
                            yparam.Value = system.Y;
                            zparam.Value = system.Z;
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
                Console.Error.WriteLine("Creating indexes");

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS SystemsSectorName ON Systems (sectorId,nameid)";
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS SystemsXZY ON Systems (x,z,y)";
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

        public void ProcessAliases(string aliaspath)
        {
            var aliasesdate = File.GetLastWriteTimeUtc(aliaspath);
            var aliases = new Dictionary<int, (int id, string system, int? mergedto)>();
            var aliasdata = File.ReadAllBytes(aliaspath);
            var jrdr = new Utf8JsonReader(aliasdata);
            jrdr.Read();
            Debug.Assert(jrdr.TokenType == JsonTokenType.StartArray);

            while (jrdr.Read() && jrdr.TokenType != JsonTokenType.EndArray)
            {
                Debug.Assert(jrdr.TokenType == JsonTokenType.StartObject);
                string system = null;
                int id = 0;
                int? mergedto = null;

                while (jrdr.Read() && jrdr.TokenType != JsonTokenType.EndObject)
                {
                    Debug.Assert(jrdr.TokenType == JsonTokenType.PropertyName);
                    var name = jrdr.GetString();
                    jrdr.Read();
                    switch ((name, jrdr.TokenType))
                    {
                        case ("system", JsonTokenType.String): system = jrdr.GetString(); break;
                        case ("id", JsonTokenType.Number): id = jrdr.GetInt32(); break;
                        case ("mergedTo", JsonTokenType.Number): mergedto = jrdr.GetInt32(); break;
                    }
                }

                if (mergedto != null)
                {
                    aliases[id] = (id, system, mergedto);
                }
            }

            foreach (var selname in Selectors.Keys)
            {
                var addaliases = new List<(int id, string system, int? mergedto)>();
                var updaliases = new List<(int id, string system, int? mergedto)>();
                var delaliases = new List<int>();
                var dbaliases = new HashSet<int>();

                var conn = CreateConnection(selname);
                conn.Open();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT edsmid, edsmid_mergedto, name FROM Aliases";

                    using var rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        int id = rdr.GetInt32(0);
                        int? mergedto = rdr.IsDBNull(1) ? (int?)null : rdr.GetInt32(1);
                        string system = rdr.GetString(2);

                        dbaliases.Add(id);

                        if (!aliases.TryGetValue(id, out var alias))
                        {
                            delaliases.Add(id);
                        }
                        else if (system != alias.system || mergedto != alias.mergedto)
                        {
                            updaliases.Add((id, system, mergedto));
                        }
                    }
                }

                foreach (var alias in aliases.Values)
                {
                    if (!dbaliases.Contains(alias.id))
                    {
                        addaliases.Add(alias);
                    }
                }

                using var txn = conn.BeginTransaction();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "DELETE FROM Aliases WHERE edsmid = @edsmid";
                    var idparam = cmd.Parameters.Add("@edsmid", SqliteType.Integer);

                    foreach (var id in delaliases)
                    {
                        idparam.Value = id;
                        cmd.ExecuteNonQuery();
                    }
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "UPDATE Aliases SET name = @name, edsmid_mergedto = @edsmid_mergedto WHERE edsmid = @edsmid";
                    var idparam = cmd.Parameters.Add("@edsmid", SqliteType.Integer);
                    var nameparam = cmd.Parameters.Add("@name", SqliteType.Text);
                    var mergeparam = cmd.Parameters.Add("@edsm_mergedto", SqliteType.Integer);

                    foreach (var (id, system, mergedto) in updaliases)
                    {
                        idparam.Value = id;
                        nameparam.Value = system;
                        mergeparam.Value = (object)mergedto ?? DBNull.Value;
                        cmd.ExecuteNonQuery();
                    }
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT INTO Aliases (edsmid, name, edsmid_mergedto) VALUES (@edsmid, @name, @edsmid_mergedto)";
                    var idparam = cmd.Parameters.Add("@edsmid", SqliteType.Integer);
                    var nameparam = cmd.Parameters.Add("@name", SqliteType.Text);
                    var mergeparam = cmd.Parameters.Add("@edsmid_mergedto", SqliteType.Integer);

                    foreach (var (id, system, mergedto) in addaliases)
                    {
                        if (system != null)
                        {
                            idparam.Value = id;
                            nameparam.Value = system;
                            mergeparam.Value = (object)mergedto ?? DBNull.Value;
                            cmd.ExecuteNonQuery();
                        }
                    }
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = txn;
                    cmd.CommandText = "INSERT OR REPLACE INTO Register (Id, ValueString) VALUES (@Id, @Val)";
                    cmd.Parameters.AddWithValue("@Id", "EDSMAliasLastDownloadTime");
                    cmd.Parameters.AddWithValue("@Val", aliasesdate.ToString("O"));
                    cmd.ExecuteNonQuery();
                }

                txn.Commit();
            }
        }
    }
}
