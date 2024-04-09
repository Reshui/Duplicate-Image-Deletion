// See https://aka.ms/new-console-template for more information
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using SQL_Functions;
using System.Threading.Tasks.Dataflow;
using System.Collections.Concurrent;

string imgFolder = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + @"\Images\Foo";

List<string> imgPaths = Directory.GetFiles(imgFolder).ToList();

string databaseName = "hashDB.db";
string hashTableName = "HashCodes";

using (var connection = new SqliteConnection($"Data Source={databaseName};Cache=Shared"))
{
    connection.Open();

    var sqlC = new SQL_Connector(connection, hashTableName, databaseName);

    HashSet<string> fileInDatabase = sqlC.GetAllFilePathsInDatabase();
    // Filter for images that haven't been processed.
    var newImagePaths = imgPaths.Where(x => !fileInDatabase.Contains(x));

    var getBytesBlock = new TransformBlock<string, FileData>(filePath => new FileData(false, false, filePath, null, rawBytes: File.ReadAllBytes(filePath)));

    ConcurrentBag<FileData> completedHashes = new();
    var computeHashBlock = new ActionBlock<FileData>((queriedFile) =>
    {
        if (queriedFile.RawBytes != null)
        {
            using var myHash = SHA512.Create();
            queriedFile.HashCode = myHash.ComputeHash(queriedFile.RawBytes);
            completedHashes.Add(queriedFile);
        }
    }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount, BoundedCapacity = 50 });

    getBytesBlock.LinkTo(computeHashBlock, new DataflowLinkOptions { PropagateCompletion = true });

    // Calculate the hash codes for the new images and store them in a FileData object.
    foreach (string imgPath in newImagePaths)
    {
        await getBytesBlock.SendAsync(imgPath);
    }
    getBytesBlock.Complete();
    await computeHashBlock.Completion;

    if (!completedHashes.IsEmpty)
    {
        sqlC.BulkInsertToDatabase(completedHashes);
    }
    Console.WriteLine($"New Images: {completedHashes.Count}\t");

    Dictionary<byte[], List<FileData>> duplicatedFilesByHashCode = sqlC.FilesWithDuplicates();

    if (duplicatedFilesByHashCode.Count > 0)
    {
        foreach (List<FileData> item in duplicatedFilesByHashCode.Values)
        {   // Loop is initialized at 1 to retain at least 1 image.
            for (int i = 1; i < item.Count; ++i)
            {
                FileData file = item[i];

                File.Delete(file.FilePath);
                file.MarkedForDeletion = true;
            }
        }

        List<FileData> filesToDelete = (from duplicatedList in duplicatedFilesByHashCode.Values
                                        from file in duplicatedList
                                        where file.MarkedForDeletion == true
                                        select file).ToList();

        sqlC.DeleteEntriesFromDatabase(filesToDelete);

        try
        {
            Console.WriteLine($"Images Deleted: {filesToDelete.Count}\nPress any key to continue.");
            Console.ReadKey();
        }
        catch (InvalidOperationException)
        {

        }
    }
}

/*static void PrintByteArray(byte[] array)
{
    for (int i = 0; i < array.Length; i++)
    {
        Console.Write($"{array[i]:X2}");
        if ((i % 4) == 3) Console.Write(" ");
    }
    Console.WriteLine();
}*/


/*

foreach ( var bucket in Interleaved(identifyImageTasks))
{   
    var completedTask = await bucket;

    var imgData = await completedTask;

    if (imgData.QueueForUpload == true) uploadQueue.Add(imgData);

    if (uploadQueue.Count == 500 || ++totalImagesProcessed == imgPaths.Count)
    {
        sqlC.BulkInsertToDatabase(uploadQueue);
        uploadQueue.Clear();
    }        
}

static Task<Task<T>>[] Interleaved<T>(IEnumerable<Task<T>> tasks)
{
    // Handles any lazy Task initiation.
    var inputTasks = tasks.ToList();

    var buckets = new TaskCompletionSource<Task<T>>[inputTasks.Count];
    var results = new Task<Task<T>>[buckets.Length];
    for (int i = 0; i < buckets.Length; i++)
    {
        buckets[i] = new TaskCompletionSource<Task<T>>();
        results[i] = buckets[i].Task;
    }

    int nextTaskIndex = -1;
    Action<Task<T>> continuation = completed =>
    {
        var bucket = buckets[Interlocked.Increment(ref nextTaskIndex)];
        bucket.TrySetResult(completed);
    };

    foreach (var inputTask in inputTasks)
        inputTask.ContinueWith(continuation, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

    return results;
}

*/