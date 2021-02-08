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
            Console.Error.WriteLine("Initializing DB");
            sysdb.Init();
            Console.Error.WriteLine("Processing systems dump");
            sysdb.LoadSystems(args[1]);
            Console.Error.WriteLine("Done");
        }
    }
}
