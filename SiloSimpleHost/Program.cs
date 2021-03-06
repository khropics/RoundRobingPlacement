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
using Microsoft.Extensions.DependencyInjection;

namespace SiloSimpleHost
{
    /// <summary>
    /// Orleans test silo host
    /// </summary>
    public class Program
    {
        /// <summary>
        /// just empty type of our new custom strategy
        /// </summary>
        [Serializable]
        public class RoundRobinPlacementStrategy : PlacementStrategy
        {

        }
        /// <summary>
        /// allows to mark grain with specific placement strategy
        /// </summary>
        [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
        public sealed class RoundRobinStrategyAttribute : PlacementAttribute
        {
            public RoundRobinStrategyAttribute() : base(new RoundRobinPlacementStrategy())
            {
            }
        }

        /// <summary>
        /// Director manages placement of the grain into the silo
        /// </summary>
        public class RoundRobinPlacementDirector : IPlacementDirector<RoundRobinPlacementStrategy>
        {
            private static ConcurrentQueue<SiloAddress> unusedSilos = new ConcurrentQueue<SiloAddress>();
            private static ConcurrentQueue<SiloAddress> usedSilos = new ConcurrentQueue<SiloAddress>();
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
                if (nextSiloAddress ==null)
                {
                    throw new ArgumentNullException("Couldn't find a compatible silo for grain");
                }
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

        /// <summary>
        /// configured type to inject all our custom services
        /// </summary>
        public class TestStartup
        {
            public IServiceProvider ConfigureServices(IServiceCollection services)
            {
                services.AddSingleton<IPlacementDirector<RoundRobinPlacementStrategy>, RoundRobinPlacementDirector>();

                return services.BuildServiceProvider();
            }
        }
        static void Main(string[] args)
        {
            // First, configure and start a local silo
            var siloConfig = ClusterConfiguration.LocalhostPrimarySilo();

            siloConfig.UseStartupType<TestStartup>(); // inject our custom service


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
            var hiGrain1 = client.GetGrain<IHello>(1);
            var hiGrain2 = client.GetGrain<IHello>(2);
            Console.WriteLine("0.");
            hiGrain.SayHello("ss");
            Console.WriteLine("1.");
            hiGrain1.SayHello("ss1");
            Console.WriteLine("2.");
            hiGrain2.SayHello("ss2");
            Console.WriteLine("3.");
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
