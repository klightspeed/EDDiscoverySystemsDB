using ICSharpCode.SharpZipLib.GZip;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
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
            -1216000,
             -960000,
             -704000,
             -576000,
             -448000,
             -320000,
             -192000,
              -64000,
               64000,
              192000,
              320000,
              448000,
              704000,
              960000,
             1216000,
             1856000,
             2496000,
             3136000,
             3776000,
             4416000,
             5056000,
             5696000,
             6336000,
             6976000,
             7616000
        };

        private readonly int[] GridX = new[]
        {
            -2496000,
            -1856000,
            -1216000,
             -960000,
             -704000,
             -448000,
             -192000,
              -64000,
               64000,
              192000,
              320000,
              448000,
              576000,
              704000,
              832000,
             1088000,
             1344000,
             1984000,
             2624000
        };

        private readonly Dictionary<(string, int), Sector> Sectors = new Dictionary<(string, int), Sector>();
        
        private readonly List<Sector> SectorList = new List<Sector>();

        private readonly Dictionary<string, int> NameIds = new Dictionary<string, int>();

        private readonly List<string> Names = new List<string>();

        private readonly List<SystemEntry> Systems = new List<SystemEntry>();

        private SqliteConnection CreateConnection()
        {
            var csb = new SqliteConnectionStringBuilder
            {
                DataSource = Path.Combine(BasePath, "EDDSystem.sqlite")
            };

            return new SqliteConnection(csb.ToString());
        }

        public void Init()
        {
            using var conn = CreateConnection();
            conn.Open();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS Register (" +
                        "ID VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "ValueInt BIGINT, " +
                        "ValueDouble DOUBLE, " +
                        "ValueString VARCHAR(100), " +
                        "ValueBlob VARBINARY(100)" +
                    ")";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS Sectors (" +
                        "ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, " +
                        "GridId INT NOT NULL, " +
                        "Name VARCHAR(100) NOT NULL COLLATE NOCASE" +
                    ")";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS Systems (" +
                        "EdsmId INT NOT NULL PRIMARY KEY, " +
                        "SectorId INT NOT NULL, " +
                        "NameId BIGINT NOT NULL, " +
                        "X INT NOT NULL, " +
                        "Y INT NOT NULL, " +
                        "Z INT NOT NULL" +
                    ")";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS Names (" +
                        "ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, " +
                        "Name VARCHAR(255) NOT NULL COLLATE NOCASE" +
                    ")";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "CREATE INDEX IF NOT EXISTS SystemsSectorName ON Systems (SectorId, NameId)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "CREATE INDEX IF NOT EXISTS SystemsXZY ON Systems (X, Z, Y)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "CREATE INDEX IF NOT EXISTS NamesName ON Names (Name)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "CREATE INDEX IF NOT EXISTS SectorName ON Sectors (Name)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "CREATE INDEX IF NOT EXISTS SectorGridId ON Sectors (GridId)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT OR IGNORE INTO Register (ID, ValueInt) VALUES ('DBVer', 200)";
                cmd.ExecuteNonQuery();
            }

            Console.Error.WriteLine("Loading sectors");
            using (var cmd = conn.CreateCommand())
            {
                var sectors = new Dictionary<int, Sector>();
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

                    sectors[sector.Id] = sector;
                }

                var maxid = sectors.Keys.OrderByDescending(e => e).FirstOrDefault();

                for (int i = 1; i <= maxid; i++)
                {
                    if (sectors.TryGetValue(i, out var sector))
                    {
                        Sectors[(sector.Name, sector.GridId)] = sector;
                    }

                    SectorList.Add(sector);
                }
            }

            Console.Error.WriteLine("Loading names");
            using (var cmd = conn.CreateCommand())
            {
                var names = new Dictionary<int, string>();
                cmd.CommandText = "SELECT Id, Name FROM Names";
                using var rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    var id = rdr.GetInt32(0);
                    names[id] = rdr.GetString(1);
                }

                var maxid = names.Keys.OrderByDescending(e => e).FirstOrDefault();

                for (int i = 1; i <= maxid; i++)
                {
                    if (names.TryGetValue(i, out var name))
                    {
                        NameIds[name] = i;
                    }

                    Names.Add(name);
                }
            }

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
                }
            }
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

        private int GetNameId(string name)
        {
            if (!NameIds.TryGetValue(name, out var id))
            {
                Names.Add(name);
                NameIds[name] = id = Names.Count;
            }

            return id;
        }

        private void ProcessSystemName(SystemEntry system, string name)
        {
            string sectorname = null;

            if (SplitProcgenName(name, out var regionname, out int mid, out int n2, out int masscode))
            {
                sectorname = regionname;
                var l1 = mid % 26;
                var l2 = (mid / 26) % 26;
                var l3 = (mid / 26 / 26) % 26;
                var n1 = mid / 26 / 26 / 26;
                system.NameId = n2 + (n1 << 16) + (masscode << 24) + ((long)l3 << 28) + ((long)l2 << 33) + ((long)l1 << 38) + (1L << 47);
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
                        system.NameId = v + (1L << 46) + ((long)dashpos << 38) + ((long)num.Length << 42);
                    }
                    else if (Surveys.Contains(nameparts[0]))
                    {
                        sectorname = nameparts[0];
                        system.NameId = GetNameId(string.Join(" ", nameparts[1..]));
                    }
                    else
                    {
                        sectorname = "NoSectorName";
                        system.NameId = GetNameId(name);
                    }
                }
                else
                {
                    sectorname = "NoSectorName";
                    system.NameId = GetNameId(name);
                }
            }

            var gx = Array.BinarySearch(GridX, system.X) + 1;
            var gz = Array.BinarySearch(GridZ, system.Z) + 1;

            if (gx < 0) gx = -gx;
            if (gz < 0) gz = -gz;
            var gridid = gz * 100 + gx;

            if (!Sectors.TryGetValue((sectorname, gridid), out var sector))
            {
                sector = new Sector { Id = SectorList.Count + 1, Name = sectorname, GridId = gridid };
                SectorList.Add(sector);
                Sectors[(sectorname, gridid)] = sector;
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
                    case ("x", JsonTokenType.Number): x = (int)(jrdr.GetDouble() * 128 + 0.5); break;
                    case ("y", JsonTokenType.Number): y = (int)(jrdr.GetDouble() * 128 + 0.5); break;
                    case ("z", JsonTokenType.Number): z = (int)(jrdr.GetDouble() * 128 + 0.5); break;
                    default: throw new InvalidOperationException();
                }
            }

            return (x, y, z);
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
            var lastsectorid = SectorList.Count;
            var lastnameid = Names.Count;
            var lastedsmid = Systems.Count;
            int num = 0;

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
                        }
                    }

                    ProcessSystemName(system, sysname);

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

            Console.Error.WriteLine();
            using var conn = CreateConnection();
            conn.Open();
            using var txn = conn.BeginTransaction();

            Console.Error.WriteLine("Saving sectors");
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO Sectors (Id, Name, GridId) VALUES (@Id, @Name, @GridId)";
                var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);
                var nameparam = cmd.Parameters.Add("@Name", SqliteType.Text);
                var grididparam = cmd.Parameters.Add("@GridId", SqliteType.Integer);

                for (int i = lastsectorid; i < SectorList.Count; i++)
                {
                    var sector = SectorList[i];

                    if (sector != default)
                    {
                        idparam.Value = sector.Id;
                        nameparam.Value = sector.Name;
                        grididparam.Value = sector.GridId;
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            Console.Error.WriteLine("Saving names");
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO Names (Id, Name) VALUES (@Id, @Name)";
                var idparam = cmd.Parameters.Add("@Id", SqliteType.Integer);
                var nameparam = cmd.Parameters.Add("@Name", SqliteType.Text);

                for (int i = lastnameid; i < Names.Count; i++)
                {
                    var name = Names[i];

                    if (name != null)
                    {
                        idparam.Value = i;
                        nameparam.Value = name;
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            Console.Error.WriteLine("Saving inserted systems");
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO Systems (EdsmId, SectorId, NameId, X, Y, Z) VALUES (@EdsmId, @SectorId, @NameId, @X, @Y, @Z)";
                var edsmidparam = cmd.Parameters.Add("@EdsmId", SqliteType.Integer);
                var sectoridparam = cmd.Parameters.Add("@SectorId", SqliteType.Integer);
                var nameidparam = cmd.Parameters.Add("@NameId", SqliteType.Integer);
                var xparam = cmd.Parameters.Add("@X", SqliteType.Integer);
                var yparam = cmd.Parameters.Add("@Y", SqliteType.Integer);
                var zparam = cmd.Parameters.Add("@Z", SqliteType.Integer);

                num = 0;
                foreach (var system in addsystems)
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

            Console.Error.WriteLine($" {num}");
            Console.Error.WriteLine("Updating systems");
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "UPDATE Systems SET SectorId = @SectorId, NameId = @NameId, X = @X, Y = @Y, Z = @Z WHERE EdsmId = @EdsmId";
                var edsmidparam = cmd.Parameters.Add("@EdsmId", SqliteType.Integer);
                var sectoridparam = cmd.Parameters.Add("@SectorId", SqliteType.Integer);
                var nameidparam = cmd.Parameters.Add("@NameId", SqliteType.Integer);
                var xparam = cmd.Parameters.Add("@X", SqliteType.Integer);
                var yparam = cmd.Parameters.Add("@Y", SqliteType.Integer);
                var zparam = cmd.Parameters.Add("@Z", SqliteType.Integer);

                num = 0;

                foreach (var system in updsystems)
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

            Console.WriteLine($" {num}");
            Console.Error.WriteLine("Appending systems");
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                cmd.CommandText = "INSERT INTO Systems (EdsmId, SectorId, NameId, X, Y, Z) VALUES (@EdsmId, @SectorId, @NameId, @X, @Y, @Z)";
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

                    if (system != default)
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
            Console.Error.WriteLine("Committing");
            txn.Commit();
        }
    }
}
