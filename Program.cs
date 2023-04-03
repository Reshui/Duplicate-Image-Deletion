// See https://aka.ms/new-console-template for more information
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
//using System.Collections.Concurrent;

using SQL_Functions;

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
    imgPaths = imgPaths.Where(x => !fileInDatabase.Contains(x)).ToList();

    var imgFileData = new List<FileData>();

    using (var myHash = SHA512.Create())
    {
        foreach (var img in imgPaths)
        {
            using (var stream = File.OpenRead(img))
            {
                byte[] computedHash = myHash.ComputeHash(stream);
                imgFileData.Add(new FileData(false, false, img, computedHash));
            }
        }
    }

    if (imgFileData.Count > 0) sqlC.BulkInsertToDatabase(imgFileData);

    Dictionary<byte[], List<FileData>> duplicatedFiles = sqlC.FilesWithDuplicates();

    string fileCountStatement = $"New Images: {imgFileData.Count}\t";

    if (duplicatedFiles.Count > 0)
    {
        foreach (List<FileData> item in duplicatedFiles.Values)
        {   // Loop is initialized at 1 to retain at least 1 image.
            for (int i = 1; i < item.Count; i++)
            {
                FileData file = item[i];

                File.Delete(file.FilePath);
                file.MarkedForDeletion = true;
            }
        }

        List<FileData> filesToDelete = (from duplicatedList in duplicatedFiles.Values
                                        from file in duplicatedList
                                        where file.MarkedForDeletion == true
                                        select file).ToList();

        sqlC.DeleteEntriesFromDatabase(filesToDelete);

        try
        {
            Console.WriteLine($"{fileCountStatement} Images Deleted: {filesToDelete.Count}\nPress any key to continue.");
            Console.ReadKey();
        }
        catch (InvalidOperationException)
        {

        }
    }
    else
    {
        Console.WriteLine(fileCountStatement);
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