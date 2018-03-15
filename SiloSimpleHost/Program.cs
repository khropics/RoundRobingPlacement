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
            private static ConcurrentDictionary<SiloAddress, ulong> avaliableSilos = new ConcurrentDictionary<SiloAddress, ulong>();
            public static ulong GetStats()
            {
                return avaliableSilos.First().Value;
            }
            public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
            {
                /*
                 1. always get list of available silos and try to add them to dictionary as they can change on fly possibly
                 2. get least silo
                 3. increment silo counter
                 4. always await
                 */

                //fill it with silos/ alwa
                var globalSilos = context.GetCompatibleSilos(target);

                foreach (var silo in globalSilos)
                {
                    avaliableSilos.TryAdd(silo, 0L);
                }

                //TODO: check if siloAddres is still compatible
                SiloAddress nextSiloAddress = null;
                nextSiloAddress = avaliableSilos.OrderBy(x => x.Value).First().Key; //get least loaded silo

                avaliableSilos[nextSiloAddress]++;  //increment usage of silo

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
                var ss = string.Format("Wololo {0}", msg);
                return Task.FromResult(ss);
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


        static async Task DoTheGoodJob(IClusterClient client, int i)
        {



            var hiGrain = client.GetGrain<IHello>(30000 + i);
            var hiGrain2 = client.GetGrain<IHello>(40000 + i);
            var hiGrain3 = client.GetGrain<IHello>(50000 + i);
            await hiGrain.SayHello("a");
            await hiGrain2.SayHello("B");
            await hiGrain3.SayHello("C");

        }


        static async Task DoTheBadJob(IClusterClient client, int i)
        {


            var hiGrain = client.GetGrain<IHello>(i);
            var hiGrain2 = client.GetGrain<IHello>(10000 + i);
            var hiGrain3 = client.GetGrain<IHello>(20000 + i);
            await hiGrain.SayHello(string.Format("i'm here number {0}", i));
            await hiGrain2.SayHello(string.Format("i'm here number {0}", i + 100));
            await hiGrain3.SayHello(string.Format("i'm here number {0}", i + 200));

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


            Task.Run(async () =>
            {
                for (var i = 1; i <= 5000; i++)
                {

                    await DoTheGoodJob(client, i);
                }
            }).ContinueWith(async prev =>
            {
                for (var i = 1; i <= 5000; i++)
                {

                    await DoTheBadJob(client, i);
                }
            });

            //if unccomment this line we'll get messed number of activations
            //Task.Run(async () =>
            //{
            //    for (var i = 1; i <= 5000; i++)
            //    {

            //        await DoTheBadJob(client, i);
            //    }
            //});



            Console.WriteLine("\nPress Enter to terminate...");
            Console.ReadLine();
            Console.WriteLine("Using async we have {0} of 30000 placements", RoundRobinPlacementDirector.GetStats());
            // Shut down
            client.Close();

            RoundRobinPlacementDirector.GetStats();
            silo.ShutdownOrleansSilo();
        }
    }
}
