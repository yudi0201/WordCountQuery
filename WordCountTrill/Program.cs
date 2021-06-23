using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.InteropServices;
using System.Xml.Xsl;
using Microsoft.StreamProcessing;


namespace WordCountTrill
{
    class Program
    {
        class WordData
        {
            public long Time;
            public string Word;

            public WordData(long time, string word)
            {
                this.Time = time;
                this.Word = word;
            }
        }

        class MyObservable : IObservable<WordData>
        {
            public IDisposable Subscribe(IObserver<WordData> observer)
            {
                using (var reader = new StreamReader(@"/root/WordCountTrill/750_thousand_word_UNIX.csv"))
                //using (var reader = new StreamReader(@"C:\Users\yudis\Documents\university\Summer2021\Code\pysparkPrograms\data\word_stream\1_million_word_UNIX.csv"))
                
                {
                  reader.ReadLine();
                  while (!reader.EndOfStream)
                  {
                      var line = reader.ReadLine();
                      var values = line.Split(',');
                      var data = new WordData(long.Parse(values[0]), values[1]);
                      observer.OnNext(data);

                  }
                }
                observer.OnCompleted();
                return Disposable.Empty;
            }
        }
        static void Main(string[] args)
        {

            var wordRecordObservable = new MyObservable();
            var wordRecordStreamable =
                wordRecordObservable.ToTemporalStreamable(e => e.Time, e => e.Time + 1)
                    .Cache();

            var sw = new Stopwatch();
            sw.Start();
            
            //int WindowSize = 10;
            var result = wordRecordStreamable.GroupApply(e => e.Word,
                s => s.Count(),
                (g, p) => new {word = g.Key, count = p});

            //result.ToStreamEventObservable()
            //    .ForEachAsync(e => Console.WriteLine(e.ToString()));

            result
                .ToStreamEventObservable()
                .Wait();

            sw.Stop();
            Console.WriteLine(sw.Elapsed.TotalSeconds);
        }
    }
    


}