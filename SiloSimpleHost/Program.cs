using System;

using Orleans;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;
using Orleans.Placement;
using Orleans.Runtime.Placement;
using Orleans.Runtime;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace SiloSimpleHost
{
    /// <summary>
    /// Orleans test silo host
    /// </summary>
    public class Program
    {
        [Serializable]
        public class RoundRobinStrategy : PlacementStrategy
        {

        }

        [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
        public sealed class RoundRobinStrategyAttribute : PlacementAttribute
        {
            public RoundRobinStrategyAttribute() : base(new RoundRobinStrategy())
            {
            }
        }

        
        public class RoundRobinDirector : IPlacementDirector<RoundRobinStrategy>
        {
            private ConcurrentQueue<SiloAddress> unusedSilos = new ConcurrentQueue<SiloAddress>();
            private ConcurrentQueue<SiloAddress> usedSilos = new ConcurrentQueue<SiloAddress>();
            public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
            {
                if (unusedSilos.Count == 0 && usedSilos.Count == 0)
                {
                    var silos = context.GetCompatibleSilos(target).OrderBy(x => x).ToArray();
                    foreach (var siloAddress in silos)
                    {
                        unusedSilos.Enqueue(siloAddress);
                    }
                }
                if (unusedSilos.Count == 0 && usedSilos.Count > 0)
                {
                    //check if all usedSilos are compatible
                    //var additionalSilos = context.GetCompatibleSilos(target).OrderBy(x => x).ToArray();

                    unusedSilos = new ConcurrentQueue<SiloAddress>(usedSilos);
                    usedSilos = new ConcurrentQueue<SiloAddress>();
                }

                //TODO: check if siloAddres is still compatible
                SiloAddress nextSiloAddress = null;
                unusedSilos.TryDequeue(out nextSiloAddress);
                usedSilos.Enqueue(nextSiloAddress);

                return Task.FromResult(nextSiloAddress);

            }
        }



        public interface IHello : IGrainWithIntegerKey
        {
            Task<string> SayHello(string msg);
        }

        [RoundRobinStrategyAttribute]
        public class HelloGrain : Grain, IHello
        {
            public Task<string> SayHello(string msg)
            {
                return Task.FromResult(string.Format("Wololo {0}", msg));
            }
        }

        static void Main(string[] args)
        {

            // First, configure and start a local silo
            var siloConfig = ClusterConfiguration.LocalhostPrimarySilo();
            var silo = new SiloHost("TestSilo", siloConfig);
            silo.InitializeOrleansSilo();
            silo.StartOrleansSilo();

            Console.WriteLine("Silo started.");

            // Then configure and connect a client.
            var clientConfig = ClientConfiguration.LocalhostSilo();
            var client = new ClientBuilder().UseConfiguration(clientConfig).Build();
            client.Connect().Wait();

            Console.WriteLine("Client connected.");
            //
            // This is the place for your test code.
            //


            var hiGrain = client.GetGrain<IHello>(0);
            var result = hiGrain.SayHello("pik pik").Result;
            Console.WriteLine(result);

            Console.WriteLine("\nPress Enter to terminate...");
            Console.ReadLine();

            // Shut down
            client.Close();
            silo.ShutdownOrleansSilo();
        }
    }
}
