using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace ConcurrencyTest;
internal class Program
{
    static int countCompact = 0;
    static void Main(string[] args)
    {
        //Stopwatch stopwatch = new Stopwatch();
        //SpinWait spinWait = new SpinWait();
        //stopwatch.Start();
        //var spTime1 = stopwatch.Elapsed;
        //spinWait.SpinOnce();
        //var spTime2 = stopwatch.Elapsed;
        //stopwatch.Stop();
        //Console.WriteLine((spTime2 - spTime1).Ticks);
        //Console.WriteLine((spTime2 - spTime1).TotalMilliseconds);

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


            //ConcurrentQueue<Info> infoCQueue = new();
            LinkedList<MixInfo> mixInfoLinkedList = new();


            AutoResetEvent mainAutoResetEvent = new(false);
            Task task = new Task(() =>
            {
                int maxCount = 0;
                int maxCompact = 1;
                int maxTotalCompact = 1;
                Stopwatch sp = new Stopwatch();
                sp.Start();
                while (isProcessing)
                {
                    mainAutoResetEvent.WaitOne();
                    if (!isProcessing)
                        break;


                    //if (infoCQueue.Count == 0)
                    //    continue;

                    //maxCount = Math.Max(maxCount, infoCQueue.Count);
                    //if (infoCQueue.TryPeek(out var info))
                    //{
                    //    lock (lockProcessing)
                    //    {
                    //        lock (info.obj)
                    //        {
                    //            if (!info.timeOut)
                    //            {
                    //                if (!processing.Overlaps(info.datas))
                    //                {
                    //                    if (!infoCQueue.TryDequeue(out var tmp))
                    //                        Console.WriteLine("ERROR TryDequeue 1");
                    //                    processing.UnionWith(info.datas);
                    //                    Interlocked.Increment(ref countCompact);
                    //                    maxCompact = Math.Max(maxCompact, countCompact);

                    //                    info.Call();

                    //                    mainAutoResetEvent.Set();
                    //                    info.isProcessing = true;
                    //                }
                    //            }
                    //            else
                    //            {
                    //                if (!infoCQueue.TryDequeue(out var tmp))
                    //                    Console.WriteLine("ERROR TryDequeue 2");
                    //                Console.WriteLine("ERROR Timeout");
                    //                if (infoCQueue.Count > 0)
                    //                    mainAutoResetEvent.Set();
                    //            }
                    //        }
                    //    }
                    //}
                    //else
                    //    mainAutoResetEvent.Set();


                    lock (lockProcessing)
                    {
                        if (mixInfoLinkedList.Count == 0)
                            continue;
                    }
                    maxCount = Math.Max(maxCount, mixInfoLinkedList.Count);
                    lock (lockProcessing)
                    {
                        var mixinfo = mixInfoLinkedList.First;
                        var keys = mixinfo.Value.keys;
                        var infos = mixinfo.Value.infos;
                        if (!processing.Overlaps(keys))
                        {
                            maxCompact = Math.Max(maxCompact, infos.Count);
                            maxTotalCompact = Math.Max(maxTotalCompact, mixInfoLinkedList.Sum(n => n.infos.Count - 1));

                            processing.UnionWith(keys);
                            mixInfoLinkedList.RemoveFirst();
                            while (infos.Count > 0)
                            {
                                var info = infos.Pop();
                                lock (info.obj)
                                {
                                    if (!info.timeOut)
                                    {
                                        info.Call();
                                        info.isProcessing = true;
                                    }
                                    else
                                    {
                                        if (!processing.IsSupersetOf(info.datas))
                                            Console.WriteLine("ERROR Not Superset m1");
                                        processing.ExceptWith(info.datas);
                                        Console.WriteLine("ERROR Timeout");
                                        if (mixInfoLinkedList.Count > 0)
                                            mainAutoResetEvent.Set();
                                    }
                                }
                            }
                        }
                    }


                }
                mainAutoResetEvent.Dispose();
                Console.WriteLine($"maxCount:{maxCount}");
                Console.WriteLine($"maxCompact:{maxCompact}");
                Console.WriteLine($"maxTotalCompact:{maxTotalCompact}");
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
                    SpinWait.SpinUntil(() => false, 1000);

                    Random rand = new();
                    Stopwatch sp = new Stopwatch();
                    sp.Start();
                    while (sp.ElapsedMilliseconds <= 3000)
                    {
                        HashSet<string> datas = new();
                        while (datas.Count < 50)
                        {
                            var arrIdx = rand.Next(0, arr.Length);
                            var id = rand.Next(1_000_000_000, 1_000_001_000);
                            datas.Add($"{arr[arrIdx]}:{id}");
                        }
                        Info info = new Info()
                        {
                            datas = datas
                        };


                        //infoCQueue.Enqueue(info);

                        lock (lockProcessing)
                        {
                            var cur = mixInfoLinkedList.First;
                            bool added = false;
                            while (cur != null)
                            {
                                if (!cur.Value.keys.Overlaps(info.datas))
                                {
                                    cur.Value.keys.UnionWith(info.datas);
                                    cur.Value.infos.Push(info);
                                    added = true;
                                    break;
                                }
                                cur = cur.Next;
                            }
                            if (!added)
                            {
                                var mixInfo = new MixInfo();
                                mixInfo.keys.UnionWith(info.datas);
                                mixInfo.infos.Push(info);
                                mixInfoLinkedList.AddLast(mixInfo);
                            }
                        }


                        mainAutoResetEvent.Set();

                        var got = info.Wait(3000);
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
                            info.DisposeTimer();
                        }
                        var handling = false;
                        if (got)
                        {
                            handling = true;
                        }
                        if (handling)
                        {
                            //Task.Delay(100).Wait();
                            SpinWait.SpinUntil(() => false, 100);

                            lock (lockProcessing)
                            {
                                if (!processing.IsSupersetOf(datas))
                                    Console.WriteLine("ERROR IsSupersetOf");
                                ;
                                processing.ExceptWith(datas);
                                if (processing.Overlaps(datas))
                                    Console.WriteLine("ERROR Overlaps 2");

                                Interlocked.Decrement(ref countCompact);
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
    class MixInfo
    {
        public HashSet<string> keys = new();
        public Stack<Info> infos = new();
    }
    class Info
    {
        public object obj = new();
        public HashSet<string> datas;
        public bool isProcessing = false;
        public bool timeOut = false;

        public ManualResetEventSlim manualResetEventSlim = new(false);
        public void Call() => manualResetEventSlim.Set();
        public bool Wait(int millisecondsTimeout) => manualResetEventSlim.Wait(millisecondsTimeout);
        public void DisposeTimer() => manualResetEventSlim.Dispose();


        //public SemaphoreSlim semaphoreSlim = new(0, 1);
        //public void Call() => semaphoreSlim.Release(1);
        //public bool Wait(int millisecondsTimeout) => semaphoreSlim.Wait(millisecondsTimeout);
        //public void DisposeTimer() => semaphoreSlim.Dispose();


    }
}
