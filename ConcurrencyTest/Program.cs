using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;

namespace ConcurrencyTest;
internal class Program
{
    static void Main(string[] args)
    {
        // 多重Key鎖定 並行處理
        do
        {
            List<string> strs = new();
            for (int i = 0; i < 10; i++)
                strs.Add(string.Join("", Enumerable.Repeat("a" + i, 40)));
            var arr = strs.ToArray();

            bool isProcessing = true;
            object lockProcessing = new();
            HashSet<string> processing = new(10000);
            ConcurrentQueue<Info> waitingInfos = new();
            AutoResetEvent mainAutoResetEvent = new(false);
            Task task = new Task(() =>
            {
                int maxCount = 0;
                Stopwatch sp = new Stopwatch();
                sp.Start();
                while (isProcessing)
                {
                    mainAutoResetEvent.WaitOne();
                    if (!isProcessing)
                        break;
                    if (waitingInfos.Count == 0)
                        continue;
                    maxCount = Math.Max(maxCount, waitingInfos.Count);
                    if (waitingInfos.TryPeek(out var info))
                    {
                        lock (lockProcessing)
                        {
                            lock (info.obj)
                            {
                                if (!info.timeOut)
                                {
                                    if (!processing.Overlaps(info.datas))
                                    {
                                        if (!waitingInfos.TryDequeue(out var tmp))
                                            Console.WriteLine("ERROR TryDequeue 1");
                                        info.isProcessing = true;
                                        processing.UnionWith(info.datas);

                                        info.manualResetEventSlim.Set();
                                        //info.semaphoreSlim.Release(1);

                                        mainAutoResetEvent.Set();
                                    }
                                }
                                else
                                {
                                    if (!waitingInfos.TryDequeue(out var tmp))
                                        Console.WriteLine("ERROR TryDequeue 2");
                                    Console.WriteLine("ERROR Timeout");
                                    if (waitingInfos.Count > 0)
                                        mainAutoResetEvent.Set();
                                }
                            }
                        }
                    }
                    else
                        mainAutoResetEvent.Set();
                }
                mainAutoResetEvent.Dispose();
                Console.WriteLine($"maxCount:{maxCount}");
            });
            task.Start();

            ConcurrentQueue<string> successes = new();
            ConcurrentQueue<string> fails = new();
            ConcurrentQueue<int> ends = new();
            var parallelCount = 100;
            Task[] tasks = new Task[parallelCount];
            for (int processNumber = 0; processNumber < parallelCount; processNumber++)
            {
                tasks[processNumber] = new Task(() =>
                {
                    Console.WriteLine($"Start");
                    Task.Delay(700).Wait();
                    Random rand = new();

                    Stopwatch sp = new Stopwatch();
                    sp.Start();
                    while (sp.ElapsedMilliseconds <= 3000)
                    {
                        HashSet<string> datas = new();
                        while (datas.Count < 50)
                        {
                            var arrIdx = rand.Next(0, arr.Length);
                            var id = rand.Next(1_000_000_000, 1_000_000_005);
                            datas.Add($"{arr[arrIdx]}:{id}");
                        }

                        Info info = new Info()
                        {
                            datas = datas
                        };

                        waitingInfos.Enqueue(info);
                        mainAutoResetEvent.Set();

                        var got = info.manualResetEventSlim.Wait(3000);
                        //var got = info.semaphoreSlim.Wait(3000);
                        if (!got)
                        {
                            lock (info.obj)
                            {
                                if (info.isProcessing)
                                {
                                    got = true;
                                }
                                else
                                {
                                    info.timeOut = true;
                                }
                            }
                        }
                        lock (info.obj)
                        {
                            info.manualResetEventSlim.Dispose();
                            info.manualResetEventSlim = null;

                            //info.semaphoreSlim.Dispose();
                            //info.semaphoreSlim = null;
                        }
                        var handling = false;
                        if (got)
                        {
                            handling = true;
                        }
                        if (handling)
                        {
                            Task.Delay(10).Wait();
                            lock (lockProcessing)
                            {
                                if (!processing.IsSupersetOf(datas))
                                    Console.WriteLine("ERROR IsSupersetOf");
                                ;
                                processing.ExceptWith(datas);
                                if (processing.Overlaps(datas))
                                    Console.WriteLine("ERROR Overlaps 2");

                                handling = false;
                            }
                            mainAutoResetEvent.Set();
                            successes.Enqueue("success");
                        }
                        else
                        {
                            fails.Enqueue("Fail");
                        }

                    }
                    ends.Enqueue(processNumber);
                    Console.WriteLine($"Not End:{parallelCount - ends.Count}");
                });
            }
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i].Start();
            }
            Task.WaitAll(tasks);

            Console.WriteLine($"successes:{successes.Count},fails:{fails.Count}");
            isProcessing = false;
            mainAutoResetEvent.Set();

            Console.WriteLine("Q?");
        }
        while ((Console.ReadKey().Key != ConsoleKey.Q));
    }
    class Info
    {
        public object obj = new();
        public ManualResetEventSlim manualResetEventSlim = new(false);
        //public SemaphoreSlim semaphoreSlim = new(0, 1);
        public HashSet<string> datas;
        public bool isProcessing = false;
        public bool timeOut = false;
    }
}
