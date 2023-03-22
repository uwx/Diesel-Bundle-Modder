using DieselEngineFormats.Bundle;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PDBundleModPatcher
{
    public class ListEntryOption
    {
        public string Title { get; set; }

        public Func<BundleExtraction, PackageFileEntry, string> StringFunc { get; set; }

        public override string ToString()
        {
            return Title;
        }
    }

    public class ListBundleOption : ListEntryOption
    {
        public new Func<BundleExtraction, PackageHeader, string, string> StringFunc { get; set; }
    }

    public struct ListOptions
    {
        public List<ListEntryOption> EntryInfo { get; set; }

        public List<ListBundleOption> BundleInfo { get; set; }

        public ListOptions(CheckedListBox.CheckedItemCollection options)
        {
            EntryInfo = new List<ListEntryOption>();
            BundleInfo = new List<ListBundleOption>();
            foreach (object obj in options)
            {
                ListEntryOption opt = (ListEntryOption)obj;
                if (opt is ListBundleOption)
                    BundleInfo.Add((ListBundleOption)opt);
                else
                    EntryInfo.Add(opt);
            }
        }
    }

    public class BundleExtraction
    {
        public readonly Queue<string> Log = new();
        public uint TotalBundle { get; set; }
        public uint CurrentBundle { get; set; }
        public string CurrentBundleName { get; set; }
        public uint CurrentBundleProgress { get; set; }
        public int CurrentBundleTotalProgress { get; set; }
        public bool Finished { get; set; }
        private readonly bool _list;
        private readonly string[] _singleBundle;
        internal readonly string ExtractFolder;
        public ListOutputter ListOutput;
        private readonly ConcurrentDictionary<uint, string> _cachedPaths = new();
        internal bool IsTerminated = false;
        private static readonly object LogLock = new();

        public BundleExtraction(string[] singleBundle, bool list, CheckedListBox.CheckedItemCollection listInfo, string listFormatter)
        {
            _list = list;
            _singleBundle = singleBundle;

            if (String.IsNullOrWhiteSpace(StaticStorage.settings.CustomExtractPath))
            {
                if (!Directory.Exists(Path.Combine(StaticStorage.settings.AssetsFolder, "extract")))
                    Directory.CreateDirectory(Path.Combine(StaticStorage.settings.AssetsFolder, "extract"));

                ExtractFolder = Path.Combine(StaticStorage.settings.AssetsFolder, "extract");
            }
            else
            {
                ExtractFolder = StaticStorage.settings.CustomExtractPath;
            }

            if (list)
            {
                Type outputType;
                switch (listFormatter)
                {
                    case "CSV":
                        outputType = typeof(CSVListOutputter);
                        break;
                    default:
                        outputType = typeof(ListOutputter);
                        break;
                }

                ListOutput = (ListOutputter)Activator.CreateInstance(outputType, !String.IsNullOrWhiteSpace(StaticStorage.settings.ListLogFile) ? StaticStorage.settings.ListLogFile : "./listlog.log", new ListOptions(listInfo), this);
            }
        }

        public void Start()
        {
            string[] files;
            if (_singleBundle != null)
                files = _singleBundle.Select(bundle => Path.Combine(StaticStorage.settings.AssetsFolder, bundle)).ToArray();
            else
                files = Directory.GetFiles(StaticStorage.settings.AssetsFolder, "*.bundle");

            TotalBundle = (uint)files.Length;
            
            var cts = new CancellationTokenSource();
            Parallel.ForEach(files, new ParallelOptions
            {
                CancellationToken = cts.Token,
                MaxDegreeOfParallelism = Environment.ProcessorCount
            }, file =>
            {
                if (IsTerminated)
                {
                    cts.Cancel();
                    return;
                }

                if (file.EndsWith("_h.bundle"))
                    return;

                var bundlePath = file.Replace(".bundle", "");
                var bundleId = Path.GetFileName(bundlePath);

                var bundle = new PackageHeader();
                TextWriteLine("Loading bundle header " + bundleId);
                if (!bundle.Load(file))
                {
                    TextWriteLine("Failed to parse bundle header.");
                    return;
                }
                if (bundle.Entries.Count == 0)
                    return;

                var currentBundleName = CurrentBundleName = bundleId;
                var currentBundleProgress = CurrentBundleProgress = 0;
                var currentBundleTotalProgress = CurrentBundleTotalProgress = _list && ListOutput.ListOptions.EntryInfo.Count == 0 ? 1 : bundle.Entries.Count;

                if (_list)
                {
                    ListBundle(bundle, bundleId);
                }
                else
                {
                    var thread = new BundleExtractorThread(this, currentBundleProgress, currentBundleTotalProgress);
                    
                    TextWriteLine("Extracting bundle: " + bundleId);
                    thread.ExtractBundle(bundle, bundleId);
                    if (thread.Log?.Count > 0)
                        lock (LogLock)
                            foreach (var s in thread.Log)
                                Log.Enqueue(s);
                }
                CurrentBundle++;
            });

            if (ListOutput != null)
            {
                TextWriteLine("Writing List information to file");
                ListOutput.Write();
                ListOutput = null;
            }
            Finished = true;
        }

        public string[] GetLog()
        {
            return Log.ToArray();
        }

        public void TextWriteLine(string line, params object[] extras)
        {
            lock (LogLock)
            {
                Log.Enqueue(StaticStorage.log.WriteLine(string.Format(line, extras), true));
            }
        }

        public string GetFileName(PackageFileEntry be)
        {
            var path = _cachedPaths.GetOrAdd(be.ID, id =>
            {
                var path = $"unknown_{id:x}.bin";
                var ne = StaticStorage.Index.EntryFromID(id);
                if (ne != null)
                {
                    path = ne.Path.UnHashed ?? $"{ne.Path:x}";

                    if (ne.Language != 0)
                    {
                        if (StaticStorage.Index.LanguageFromID(ne.Language) != null)
                        {
                            var langExt = StaticStorage.Index.LanguageFromID(ne.Language).Name.UnHashed;
                            path += $".{langExt ?? ne.Language.ToString("x")}";
                        }
                        else
                        {
                            path += $".{ne.Language:x}";
                        }
                    }

                    var extension = ne.Extension.UnHashed ?? $"{ne.Extension:x}";

                    if (!_list && StaticStorage.settings.ExtensionConversion.TryGetValue(extension, out var extensionReplacement))
                        extension = extensionReplacement;

                    path += "." + extension;
                }

                return path;
            });

            return path;
        }

        public void ListBundle(PackageHeader bundle, string bundleId)
        {
            ListOutput.WriteBundle(bundle, bundleId);

            if (ListOutput.ListOptions.EntryInfo.Count > 0)
            {
                for (; CurrentBundleProgress < CurrentBundleTotalProgress; CurrentBundleProgress++)
                {
                    PackageFileEntry be = bundle.Entries[(int)CurrentBundleProgress];
                    if (IsTerminated)
                        break;

                    ListOutput.WriteEntry(be);
                }
            }
            else
                CurrentBundleProgress++;
        }

        public void Terminate()
        {
            IsTerminated = true;
        }

    }

    public class BundleExtractorThread
    {
        private static readonly ArrayPool<byte> BufferPool = ArrayPool<byte>.Create(1024*1024, 50);

        public Queue<string> Log;

        private readonly BundleExtraction _parent;
        
        private readonly uint _currentBundleProgress;
        private readonly int _currentBundleTotalProgress;

        public BundleExtractorThread(BundleExtraction parent, uint currentBundleProgress, int currentBundleTotalProgress)
        {
            _parent = parent;
            _currentBundleProgress = currentBundleProgress;
            _currentBundleTotalProgress = currentBundleTotalProgress;
        }

        public void TextWriteLine(string line, params object[] extras)
        {
            (Log ??= new Queue<string>()).Enqueue(StaticStorage.log.WriteLine(string.Format(line, extras), true));
        }

        public void ExtractBundle(PackageHeader bundle, string bundleId)
        {
            var bundleFile = Path.Combine(StaticStorage.settings.AssetsFolder, bundleId + ".bundle");
            if (!File.Exists(bundleFile))
            {
                const string errorMessage = "Bundle file does not exist.";
                MessageBox.Show(errorMessage);
                TextWriteLine(errorMessage);
                return;
            }

            Parallel.For(_currentBundleProgress, _currentBundleTotalProgress, currentBundleProgress =>
            {
                var be = bundle.Entries[(int)currentBundleProgress];
                //if (self._parent.IsTerminated)
                //    break;

                var path = Path.Combine(_parent.ExtractFolder, _parent.GetFileName(be));

                if (StaticStorage.settings.IgnoreExistingFiles && File.Exists(path))
                    return;

                var folder = Path.GetDirectoryName(path);

                if (!string.IsNullOrWhiteSpace(folder) && !Directory.Exists(folder))
                    Directory.CreateDirectory(folder);

                if (be.Length == 0 && File.Exists(path))
                    return;

                FileStream outStream;

                try
                {
                    outStream = File.OpenWrite(path);
                }
                catch (IOException)
                {
                    return;
                }

                using (outStream)
                {
                    using var fs = new FileStream(bundleFile, FileMode.Open, FileAccess.Read);
                    fs.Position = be.Address;

                    if (be.Length == -1)
                        CopyStream(fs, outStream, (int)(fs.Length - fs.Position));
                    else
                        CopyStream(fs, outStream, be.Length);
                }
            });
        }
        
        private static void CopyStream(Stream input, Stream output, int bytes)
        {
            const int bufferLen = 1024*1024;
            var buffer = BufferPool.Rent(bufferLen);
            int read;
            while (bytes > 0 && (read = input.Read(buffer, 0, Math.Min(bufferLen, bytes))) > 0)
            {
                output.Write(buffer, 0, read);
                bytes -= read;
            }
        }
    }

    public class ListOutputter
    {
        protected StringBuilder Output = new StringBuilder();

        protected string OutputFile;

        public ListOptions ListOptions { get; set; }

        protected dynamic Parent;

        public ListOutputter(string output_file, ListOptions options, dynamic parent)
        {
            OutputFile = output_file;
            ListOptions = options;
            Parent = parent;
            WriteTitle();
        }

        public virtual void WriteTitle()
        {
            for (int i = 0; i < ListOptions.EntryInfo.Count; i++)
            {
                ListEntryOption opt = ListOptions.EntryInfo[i];
                Output.Append((i == 0 ? "" : " - ") + opt.Title);
            }
            Output.AppendLine();
        }

        public virtual void WriteEntry(PackageFileEntry entry)
        {
            if (ListOptions.EntryInfo.Count == 0)
                return;

            Output.Append("\t");
            for (int i = 0; i < ListOptions.EntryInfo.Count; i++)
            {
                ListEntryOption opt = ListOptions.EntryInfo[i];
                Output.Append((i == 0 ? "" : " - ") + opt.StringFunc(Parent, entry));
            }
            Output.AppendLine();
        }

        public virtual void WriteBundle(PackageHeader header, string bundle_id)
        {
            if (ListOptions.BundleInfo.Count == 0)
                return;

            Output.AppendLine();
            for (int i = 0; i < ListOptions.BundleInfo.Count; i++)
            {
                ListBundleOption opt = ListOptions.BundleInfo[i];
                Output.Append((i == 0 ? "" : " - ") + opt.StringFunc(Parent, header, bundle_id));
            }
            Output.AppendLine((ListOptions.EntryInfo.Count > 0 ? ":" : ""));
        }

        public void Write()
        {
            File.WriteAllText(OutputFile, Output.ToString());
        }
    }

    public class CSVListOutputter : ListOutputter
    {
        public CSVListOutputter(string output_file, ListOptions options, dynamic parent) : base(output_file, options, (BundleExtraction)parent) { }

        private bool WriteEmptyBundleColumns = false;

        public override void WriteTitle()
        {
            int i = 0;

            foreach (ListEntryOption opt in ListOptions.BundleInfo)
            {
                Output.Append((i == 0 ? "" : ",") + "\"" + opt.Title + "\"");
                i++;
            }

            foreach (ListEntryOption opt in ListOptions.EntryInfo)
            {
                Output.Append((i == 0 ? "" : ",") + "\"" + opt.Title + "\"");
                i++;
            }
            Output.AppendLine();
        }

        public override void WriteBundle(PackageHeader header, string bundle_id)
        {
            if (ListOptions.BundleInfo.Count == 0)
                return;

            Output.AppendLine();
            for (int i = 0; i < ListOptions.BundleInfo.Count; i++)
            {
                ListBundleOption opt = ListOptions.BundleInfo[i];
                Output.Append((i == 0 ? "" : ",") + "\"" + opt.StringFunc(Parent, header, bundle_id) + "\"");
            }
            WriteEmptyBundleColumns = false;
        }

        public override void WriteEntry(PackageFileEntry entry)
        {
            if (ListOptions.EntryInfo.Count == 0)
                return;

            if (WriteEmptyBundleColumns && ListOptions.BundleInfo.Count > 0)
            {
                for (int i = 0; i < ListOptions.BundleInfo.Count - 1; i++)
                {
                    Output.Append(",");
                }
            }

            for (int i = 0; i < ListOptions.EntryInfo.Count; i++)
            {
                ListEntryOption opt = ListOptions.EntryInfo[i];
                Output.Append((i == 0 && ListOptions.BundleInfo.Count == 0 ? "" : ",") + "\"" + opt.StringFunc(Parent, entry) + "\"");
            }
            Output.AppendLine();
            WriteEmptyBundleColumns = true;
        }
    }
}
