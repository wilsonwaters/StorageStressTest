using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using CommandLine;

namespace StorageStressTest
{
    class StorageStressTest
    {
        static void Main(string[] args)
        {
            CommandLine.Parser.Default.ParseArguments<Options>(args)
                .WithParsed<Options>(opts => RunOptionsAndReturnExitCode(opts))
                .WithNotParsed<Options>((errs) => HandleParseError(errs));
            StorageStressTest tester = new StorageStressTest();
            tester.SetOptions(opts);
            tester.run();
        }

        static int RunOptionsAndReturnExitCode(Options options)
        {
            opts = options;
            return 0;
        }

        static void HandleParseError(IEnumerable<Error> errs)
        {
            Console.WriteLine("Error "+errs);
            System.Environment.Exit(1);
        }

        public static Options opts;
        public class Options
        {
            [Option('l', "log", Required = false, HelpText = "File to append test results", Default = "results.csv")]
            public string log { get; set; }

            [Option('f', "folder", Required = true, HelpText = "Folder where tests will occur")]
            public string folder { get; set; }

            [Option('t', "threads", Required = false, HelpText = "number of threads to spawn", Default = 1)]
            public int threads { get; set; }

            [Option('d', "duration", Required = false, HelpText = "number of seconds to test", Default = 60)]
            public int duration { get; set; }

            [Option('c', "createfiles", Required = false, HelpText = "number of files to create for FileOpens test", Default = 10000)]
            public int createfiles { get; set; }

            [Option('n', "nocreatefilesnested", Required = false, HelpText = "dont create files in nexted folder structure no more than 1000 files in each folder", Default = false)]
            public bool nocreatefilesnested { get; set; }
        }

        public class Result
        {
            public long ops;
            public long bytesWritten;
            public long duration;
        }

        Options options;
        List<Result> results;
        bool stop;
        byte [] testBytes;

        public StorageStressTest()
        {

        }

        public void SetOptions(Options options)
        {
            this.options = options;
        }


        public void run()
        {
            RunCreates();
            RunOpens();
            RunCreateWrites(1024);
            RunCreateWrites(10 * 1024 * 1024);
        }

        public void RunCreates()
        {
            List<Thread> threads = new List<Thread>();
            results = new List<Result>();


            for (int i = 0; i < options.threads; i++)
            {
                Thread t = new Thread(new ParameterizedThreadStart(TestFileCreates));
                threads.Add(t);

                Result result = new Result();
                results.Add(result);
            }

            for (int i = 0; i < options.threads; i++)
            {
                TestFileCreatesPrepare(i);
            }

            stop = false;
            for (int i = 0; i < options.threads; i++)
            {
                threads[i].Start(i);
            }
            Thread.Sleep(1000 * options.duration);
            stop = true;
            threads.ForEach(t => t.Join());

            long totalCreates = 0;
            results.ForEach(r => totalCreates += r.ops);
            WriteLogs("FileCreate", options.threads, results[0].duration, totalCreates);
        }

        public void RunOpens()
        {
            List<Thread> threads = new List<Thread>();
            results = new List<Result>();

            for (int i = 0; i < options.threads; i++)
            {
                TestFileOpensPrepare(i);
            }

            for (int i = 0; i < options.threads; i++)
            {
                Thread t = new Thread(new ParameterizedThreadStart(TestFileOpens));
                threads.Add(t);

                Result result = new Result();
                results.Add(result);
            }

            stop = false;
            for (int i = 0; i < options.threads; i++)
            {
                threads[i].Start(i);
            }
            Thread.Sleep(1000 * options.duration);
            stop = true;
            threads.ForEach(t => t.Join());

            long totalOpens = 0;
            results.ForEach(r => totalOpens += r.ops);
            WriteLogs("FileOpens", options.threads, results[0].duration, totalOpens);
        }

        public void RunCreateWrites(int bytes)
        {
            List<Thread> threads = new List<Thread>();
            results = new List<Result>();

            for (int i = 0; i < options.threads; i++)
            {
                TestFileCreateWritesPrepare(i, bytes);
            }

            for (int i = 0; i < options.threads; i++)
            {
                Thread t = new Thread(new ParameterizedThreadStart(TestFileCreateWrites));
                threads.Add(t);

                Result result = new Result();
                results.Add(result);
            }

            stop = false;
            for (int i = 0; i < options.threads; i++)
            {
                threads[i].Start(i);
            }
            Thread.Sleep(1000 * options.duration);
            stop = true;
            threads.ForEach(t => t.Join());

            long totalCreates = 0;
            long totalBytesWritten = 0;
            results.ForEach(r => totalCreates += r.ops);
            results.ForEach(r => totalBytesWritten += r.bytesWritten);
            WriteLogs("FileCreateWrites("+bytes+"B)", options.threads, results[0].duration, totalCreates, totalBytesWritten);
        }

       


        public void TestFileCreatesPrepare(object threadNumObj)
        {
            int threadNum = (int)threadNumObj;
            string folder = options.folder + "\\FileCreates" + threadNum + "\\";
            if (System.IO.Directory.Exists(folder))
                System.IO.Directory.Delete(folder, true);
            System.IO.Directory.CreateDirectory(folder);
        }

        public void TestFileCreates(object threadNumObj)
        {
            int threadNum = (int)threadNumObj;
            long numCreates = 0;
            string folder = options.folder + "\\FileCreates" + threadNum + "\\";
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            while (!stop)
            {
                string file = folder + numCreates;
                if (!options.nocreatefilesnested)
                {
                    string folderNested = folder + "\\" + numCreates / 1000;
                    System.IO.Directory.CreateDirectory(folderNested);
                    file = folderNested + "\\" + numCreates;
                }
                using (System.IO.File.Create(file)) ;
                numCreates++;
            }
            sw.Stop();

            results[threadNum].ops = numCreates;
            results[threadNum].duration = sw.ElapsedMilliseconds;
        }


        public void TestFileOpensPrepare(object threadNumObj)
        {
            int threadNum = (int)threadNumObj;
            string folder = options.folder + "\\FileOpens" + threadNum + "\\";
            //if (System.IO.Directory.Exists(folder))
            //    System.IO.Directory.Delete(folder, true);
            System.IO.Directory.CreateDirectory(folder);

            for (int i = 0; i < options.createfiles; i++)
            {
                string file = folder + i;
                if (!options.nocreatefilesnested)
                {
                    string folderNested = folder + "\\" + i / 1000;
                    System.IO.Directory.CreateDirectory(folderNested);
                    file = folderNested + "\\" + i;
                }
                if (!System.IO.File.Exists(file))
                    using (System.IO.File.Create(file)) ;
            }
        }

        public void TestFileOpens(object threadNumObj)
        {
            int threadNum = (int)threadNumObj;
            long numOpens = 0;
            string folder = options.folder + "\\FileOpens" + threadNum + "\\";
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            while (!stop)
            {
                string file = folder + numOpens % options.createfiles;
                if (!options.nocreatefilesnested)
                {
                    long i = numOpens % options.createfiles;
                    string folderNested = folder + "\\" + i / 1000;
                    file = folderNested + "\\" + i;
                }
                using (System.IO.File.Create(file)) ;
                numOpens++;
            }
            sw.Stop();

            results[threadNum].ops = numOpens;
            results[threadNum].duration = sw.ElapsedMilliseconds;
        }



        public void TestFileCreateWritesPrepare(object threadNumObj, long bytes)
        {
            int threadNum = (int)threadNumObj;
            string folder = options.folder + "\\FileCreateWrites" + threadNum + "\\";
            if (System.IO.Directory.Exists(folder))
            {
                System.IO.Directory.Delete(folder, true);
                int count = 0;
                while (count < 10 && System.IO.Directory.Exists(folder))
                {
                    Thread.Sleep(1000); // gah, windows. Deleted gets done in the background it seems. 
                    if (System.IO.Directory.Exists(folder))
                        System.IO.Directory.Delete(folder, true);
                    count++;
                }
                if (System.IO.Directory.Exists(folder))
                    throw new Exception("can't delete folder " + folder);
            }
            System.IO.Directory.CreateDirectory(folder);

            Random rnd = new Random();
            testBytes = new byte[bytes];
            rnd.NextBytes(testBytes);

            
        }

        public void TestFileCreateWrites(object threadNumObj)
        {
            int threadNum = (int)threadNumObj;
            long numCreates = 0;
            string folder = options.folder + "\\FileCreateWrites" + threadNum + "\\";
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            while (!stop)
            {
                string file = folder + numCreates;
                if (!options.nocreatefilesnested)
                {
                    string folderNested = folder + "\\" + numCreates / 1000;
                    System.IO.Directory.CreateDirectory(folderNested);
                    file = folderNested + "\\" + numCreates;
                }
                using (Stream f = File.OpenWrite(file))
                {
                    f.Write(testBytes, 0, testBytes.Length);
                    numCreates++;
                }
            }
            sw.Stop();

            results[threadNum].ops = numCreates;
            results[threadNum].bytesWritten = numCreates * testBytes.Length;
            results[threadNum].duration = sw.ElapsedMilliseconds;
        }


        /// <summary>
        /// File format is 
        /// Operation, numThreads, time, OPS, IOPS, bytes, MBytesPerSec
        /// </summary>
        /// <param name="message"></param>
        public void WriteLogs(string operation, int numThreads, long durationMs, long ops, long bytes=0)
        {
            using (System.IO.StreamWriter log = new System.IO.StreamWriter(options.log, true))
            {
                double iops = ops / (durationMs / 1000.0);
                double mbytesPerSec = (bytes / (durationMs / 1000.0))/(1024*1024);
                log.WriteLine(operation + ", " + numThreads + ", " + durationMs + ", " + ops + ", " + iops + ", " + bytes + ", " + mbytesPerSec);
                System.Console.WriteLine(operation + ", " + numThreads + ", " + durationMs + ", " + ops + ", " + iops + ", " + bytes + ", " + mbytesPerSec);
            }
        }


    }
}
