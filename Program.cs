using System;

namespace EDDiscoverySystemsDB
{
    class Program
    {
        static void Main(string[] args)
        {
            var sysdb = new SystemsDB();
            sysdb.BasePath = args[0];
            var edsmpath = args[1];
            var aliaspath = args.Length >= 3 ? args[2] : null;
            var namespath = args.Length >= 4 ? args[3] : null;

            if (!string.IsNullOrEmpty(namespath))
            {
                sysdb.LoadNames(namespath);
            }

            Console.Error.WriteLine("Initializing DB");
            sysdb.Init();
            Console.Error.WriteLine("Processing systems dump");
            sysdb.LoadSystems(edsmpath);
            Console.Error.WriteLine("Done");

            if (!string.IsNullOrEmpty(aliaspath))
            {
                Console.Error.WriteLine("Processing aliases");
                sysdb.ProcessAliases(aliaspath);
                Console.Error.WriteLine("Done");
            }
        }
    }
}
