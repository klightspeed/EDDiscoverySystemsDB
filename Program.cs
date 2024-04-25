using System;
using System.IO;

namespace EDDiscoverySystemsDB
{
    class Program
    {
        static void Main(string[] args)
        {
            var sysDumpPath = args[1];
            var aliasPath = args.Length >= 3 ? args[2] : null;
            var namesPath = args.Length >= 4 ? args[3] : null;
            int estsyscount = 120_000_000;

            if (File.Exists(sysDumpPath))
            {
                var dumpsize = new FileInfo(sysDumpPath).Length;
                estsyscount = (int)(dumpsize / 30);
            }

            var sysdb = new SystemsDB(estsyscount)
            {
                BasePath = args[0]
            };

            if (!string.IsNullOrEmpty(namesPath) && File.Exists(namesPath))
            {
                sysdb.LoadNames(namesPath);
            }

            Console.Error.WriteLine("Initializing DB");
            sysdb.Init();
            Console.Error.WriteLine("Processing systems dump");
            sysdb.LoadSystems(sysDumpPath);
            Console.Error.WriteLine("Done");

            if (!string.IsNullOrEmpty(namesPath))
            {
                sysdb.SaveNames(namesPath);
            }
        }
    }
}
