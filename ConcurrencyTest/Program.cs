using System.Diagnostics;

namespace ConcurrencyTest;
internal class Program
{
    static void Main(string[] args)
    {
        CancellationTokenSource cancellationTokenSource = new();
        CancellationToken cancellationToken = cancellationTokenSource.Token;

        Console.WriteLine("多重Key鎖定 並行處理");
        do
        {
            int record_maxQueueCount = 0;
            int record_maxCountOfMixedWorks = 1;
            int[] record_maxMixCountOfWorkIds = new int[0];
            int record_maxProcessingKeys = 0;
            string[] record_maxProcessingKeyStrings = new string[0];
            int record_maxCountOfCompactedWorks = 1;
            int[] record_maxTotalCompactedWorkCounts = new int[0];

            object lockProcessing = new();
            HashSet<string> processing = new(10000);

            LinkedList<MixWorksInfo> mixWorksInfoLinkedList = new();

            AutoResetEventWrapper processResourceHandlerControl = new(false);
            Task.Run(() =>
            {
                Stopwatch sp = new Stopwatch();
                sp.Start();
                while (!cancellationToken.IsCancellationRequested)
                {
                    int waitResult = WaitHandle.WaitAny(new WaitHandle[] { processResourceHandlerControl.AutoResetEvent, cancellationToken.WaitHandle });

                    if (waitResult == WaitHandle.WaitTimeout || waitResult == 1)
                        break;

                    lock (lockProcessing)
                    {
                        if (mixWorksInfoLinkedList.Count == 0)
                            continue;
                    }

                    record_maxQueueCount = Math.Max(record_maxQueueCount, mixWorksInfoLinkedList.Count);

                    lock (lockProcessing)
                    {
                        var firstMixWorksInfo = mixWorksInfoLinkedList.First;

                        var firstMixKeys = firstMixWorksInfo.Value.mixKeys;
                        var firstWorkInfos = firstMixWorksInfo.Value.workInfos;

                        var hasProcessingSpaceForKeys = !processing.Overlaps(firstMixKeys);
                        if (hasProcessingSpaceForKeys)
                        {
                            if (record_maxCountOfMixedWorks < firstWorkInfos.Count)
                                record_maxMixCountOfWorkIds = firstWorkInfos.Select(n => n.Id).ToArray();
                            record_maxCountOfMixedWorks = Math.Max(record_maxCountOfMixedWorks, firstWorkInfos.Count);
                            var new_record_maxTotalCompactedWorks = mixWorksInfoLinkedList.Sum(n => n.workInfos.Count);
                            if (record_maxCountOfCompactedWorks < new_record_maxTotalCompactedWorks)
                                record_maxTotalCompactedWorkCounts = mixWorksInfoLinkedList.Select(n => n.workInfos.Count).ToArray();
                            record_maxCountOfCompactedWorks =
                                Math.Max(record_maxCountOfCompactedWorks, new_record_maxTotalCompactedWorks);

                            // add first to processing
                            processing.UnionWith(firstMixKeys);
                            mixWorksInfoLinkedList.RemoveFirst();

                            if (record_maxProcessingKeys < processing.Count)
                                record_maxProcessingKeyStrings = processing.ToArray();
                            record_maxProcessingKeys = Math.Max(record_maxProcessingKeys, processing.Count);

                            while (firstWorkInfos.Count > 0)
                            {
                                var workInfo = firstWorkInfos.Dequeue();

                                lock (workInfo.obj)
                                {
                                    if (!workInfo.timeOut)
                                    {
                                        workInfo.Call();
                                        workInfo.isProcessing = true;
                                    }
                                    else
                                    {
                                        processing.ExceptWith(workInfo.keys);
                                        Console.WriteLine("ERROR Timeout");

                                        var hasSpaceForCheckNext = mixWorksInfoLinkedList.Count > 0;
                                        if (hasSpaceForCheckNext)
                                            processResourceHandlerControl.Set();
                                    }
                                }
                            }
                        }
                    }
                }
                processResourceHandlerControl.Dispose();
            });


            var strSources = NewStrSources(10);
            int successCount = 0;
            int failCount = 0;
            ManualResetEventSlim taskBeginProcessControl = new(false);
            var taskCount = 100;
            ThreadPool.SetMinThreads(taskCount, taskCount);// here for test
            int readyCount = 0;
            int finishedCount = 0;
            var milliseconds = 3000;
            for (int processNumber = 0; processNumber < taskCount; processNumber++)
            {
                var processId = processNumber;
                Task.Run(() =>
                {
                    Interlocked.Increment(ref readyCount);
                    taskBeginProcessControl.Wait();

                    Random rand = new();
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    while (sw.ElapsedMilliseconds <= milliseconds)
                    {
                        HashSet<string> keys = new();
                        while (keys.Count < 50)
                        {
                            var strIdx = rand.Next(0, strSources.Length);
                            var id = 1_000_000_000 + rand.Next(0, 500);
                            keys.Add($"{strSources[strIdx]}_{id}");
                        }
                        WorkInfo info = new()
                        {
                            keys = keys,
                            Id = processId
                        };

                        // find space and add
                        lock (lockProcessing)
                        {
                            var curMix = mixWorksInfoLinkedList.First;
                            bool added = false;
                            while (curMix != null)
                            {
                                if (!curMix.Value.mixKeys.Overlaps(info.keys))
                                {
                                    curMix.Value.mixKeys.UnionWith(info.keys);
                                    curMix.Value.workInfos.Enqueue(info);
                                    added = true;
                                    break;
                                }
                                curMix = curMix.Next;
                            }
                            if (!added)
                            {
                                var mixInfo = new MixWorksInfo();
                                mixInfo.mixKeys.UnionWith(info.keys);
                                mixInfo.workInfos.Enqueue(info);
                                mixWorksInfoLinkedList.AddLast(mixInfo);
                            }
                        }

                        processResourceHandlerControl.Set();

                        // wait call
                        var got = false;
                        try
                        {
                            got = info.Wait(3000, cancellationToken);
                        }
                        catch
                        {
                            //throw;
                        }
                        if (cancellationToken.IsCancellationRequested)
                        {
                            break;
                        }
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
                            info.DisposeControl();
                        }

                        // process work
                        var handling = false;
                        if (got)
                        {
                            handling = true;
                        }
                        if (handling)
                        {
                            // work
                            SpinWait.SpinUntil(() => false, 0);

                            // end work
                            lock (lockProcessing)
                            {
                                processing.ExceptWith(keys);
                            }

                            //has space for check next
                            processResourceHandlerControl.Set();
                            Interlocked.Increment(ref successCount);
                        }
                        else
                        {
                            Interlocked.Increment(ref failCount);
                        }

                    }
                    Interlocked.Increment(ref finishedCount);
                });
            }

            Console.WriteLine($"tasks count:{taskCount}");
            while (readyCount < taskCount)
            {
                var pre = readyCount;
                SpinWait.SpinUntil(() => pre != readyCount || readyCount == taskCount);
                Console.Write($"\r準備task進度：{readyCount}/{taskCount}");
            }
            Console.Write($"\r準備task進度：{readyCount}/{taskCount}");
            Console.WriteLine();

            Stopwatch sw = new Stopwatch();
            sw.Start();
            taskBeginProcessControl.Set();
            while (finishedCount < taskCount)
            {
                var pre = finishedCount;
                SpinWait.SpinUntil(() => pre != finishedCount || finishedCount == taskCount);
                Console.Write($"\r完成task進度：{finishedCount}/{taskCount}");
            }
            Console.Write($"\r完成task進度：{finishedCount}/{taskCount}");
            Console.WriteLine();
            sw.Stop();
            cancellationTokenSource.Cancel();


            var recordColor = ConsoleColor.Yellow;
            Random rand = new Random();
            var caseSpace = "  ";

            Console.ForegroundColor = recordColor;
            Console.WriteLine($"Elapsed Milliseconds:{sw.ElapsedMilliseconds}");
            Console.WriteLine($"success: ● ={successCount}");
            Console.WriteLine($"fail: × ={failCount}");
            Console.WriteLine($"max queue count:{record_maxQueueCount}");
            Console.ResetColor();
            var boxChars = new char[] { '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█' };
            Console.WriteLine($"  {string.Join("", Enumerable.Repeat(' ', record_maxQueueCount).Select(n => boxChars[rand.Next(0, boxChars.Length)]))}");
            Console.ForegroundColor = recordColor;
            Console.WriteLine($"max count of mixed works:{record_maxCountOfMixedWorks}");
            Console.ResetColor();
            Console.WriteLine($"{caseSpace}{string.Join("", record_maxMixCountOfWorkIds.Select(id => $"[{id}]"))}");
            Console.ForegroundColor = recordColor;
            Console.WriteLine($"max processing keys:{record_maxProcessingKeys}");
            Console.ResetColor();
            Console.WriteLine($"{caseSpace}{string.Join("", record_maxProcessingKeyStrings.Take(3).Select(n => $"[{n}]"))}...");
            Console.ForegroundColor = recordColor;
            Console.WriteLine($"max count of compacted works:{record_maxCountOfCompactedWorks}");
            Console.ResetColor();
            var rowCount_record_maxTotalCompactedWorkCounts = record_maxTotalCompactedWorkCounts.Max();
            for (int i = 0; i < rowCount_record_maxTotalCompactedWorkCounts; i++)
            {
                Console.Write(caseSpace);
                var showHigh = rowCount_record_maxTotalCompactedWorkCounts - i;
                foreach (var workCount in record_maxTotalCompactedWorkCounts)
                {
                    if (workCount >= showHigh)
                        Console.Write('○');
                    else
                        Console.Write(' ');
                }
                Console.WriteLine();
            }

            //record_maxTotalCompactedWorkCounts

            Console.ResetColor();
            cancellationTokenSource = new();
            cancellationToken = cancellationTokenSource.Token;
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Q?");
            Console.ResetColor();
        }
        while ((Console.ReadKey().Key != ConsoleKey.Q));
    }

    private static string[] NewStrSources(int count)
    {
        List<string> strs = new();
        for (int i = 0; i < count; i++)
            strs.Add(string.Join("", Enumerable.Repeat("a" + i, 40)));
        return strs.ToArray();
    }

    class MixWorksInfo
    {
        public readonly HashSet<string> mixKeys = new();
        public readonly Queue<WorkInfo> workInfos = new();
    }
    class WorkInfo
    {
        public int Id { get; init; }
        public readonly object obj = new();
        public IReadOnlySet<string> keys { get; init; }
        public bool isProcessing = false;
        public bool timeOut = false;

        public readonly ManualResetEventSlimWrapper _control = new(false);
        public void Call()
        {
            try { _control.Set(); } catch { }
        }
        public bool Wait(int millisecondsTimeout, CancellationToken cancellationToken = default) => _control.Wait(millisecondsTimeout, cancellationToken);

        public void DisposeControl()
        {
            _control.Dispose();
        }

    }

    class ManualResetEventSlimWrapper : IDisposable
    {
        private readonly ManualResetEventSlim _manualResetEventSlim;

        public ManualResetEventSlimWrapper(bool initialState)
        {
            _manualResetEventSlim = new ManualResetEventSlim(initialState);
        }

        public void Set()
        {
            try { _manualResetEventSlim.Set(); } catch { };
        }

        public bool Wait(int millisecondsTimeout, CancellationToken cancellationToken = default) => _manualResetEventSlim.Wait(millisecondsTimeout, cancellationToken);

        private bool _isDisposed = false;
        public void Dispose()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                Dispose(true);
                GC.SuppressFinalize(this);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _manualResetEventSlim.Dispose();
            }
        }
    }

    class AutoResetEventWrapper : IDisposable
    {
        public readonly AutoResetEvent AutoResetEvent;

        public AutoResetEventWrapper(bool initialState)
        {
            AutoResetEvent = new AutoResetEvent(initialState);
        }

        public void Set()
        {
            try { AutoResetEvent.Set(); } catch { };
        }

        private bool _isDisposed = false;
        public void Dispose()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                Dispose(true);
                GC.SuppressFinalize(this);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                AutoResetEvent.Dispose();
            }
        }
    }
}
