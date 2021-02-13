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
            Console.Error.WriteLine("Initializing DB");
            sysdb.Init();
            Console.Error.WriteLine("Processing systems dump");
            sysdb.LoadSystems(edsmpath);
            Console.Error.WriteLine("Done");

            if (aliaspath != null)
            {
                Console.Error.WriteLine("Processing aliases");
                sysdb.ProcessAliases(aliaspath);
                Console.Error.WriteLine("Done");
            }
        }
    }
}
