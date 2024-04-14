using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics;
using SQL_Functions;

var watch = Stopwatch.StartNew();

string databaseName = "hashDB.db";
string pathToDesktop = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
const int MaxBounded = 50;
Console.WriteLine();

foreach (bool editWallpapers in new bool[] { true, false })
{
    string imgFolder = Path.Combine(pathToDesktop, editWallpapers ? "Wallpapers" : "Images\\Foo");
    Console.WriteLine(imgFolder);
    string hashTableName = editWallpapers ? "WallpaperHashCodes" : "HashCodes";

    using var connection = new SqliteConnection($"Data Source={databaseName};Cache=Shared");
    connection.Open();
    try
    {
        var sqlC = new SQL_Connector(connection, hashTableName, databaseName);
        int totalImagesProcessed = 0;
        /*
        var getBytesBlock = new TransformBlock<string, (FileData, byte[])>(async filePath => (new FileData(false, false, filePath, null), await File.ReadAllBytesAsync(filePath).ConfigureAwait(false)), new ExecutionDataflowBlockOptions { BoundedCapacity = MaxBounded });

        var computeHashBlock = new TransformBlock<(FileData, byte[]), FileData>(input =>
        {
            FileData queriedFile = input.Item1;
            byte[] rawBytes = input.Item2;

            using var myHash = SHA512.Create();
            queriedFile.HashCode = myHash.ComputeHash(rawBytes);

            return queriedFile;
        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount, BoundedCapacity = MaxBounded });
        */
        var computeHashBlock = new TransformBlock<string, FileData>(async filePath =>
        {
            using FileStream fileStream = File.OpenRead(filePath);
            using var myHash = SHA512.Create();
            return new FileData(false, false, filePath, await myHash.ComputeHashAsync(fileStream, CancellationToken.None).ConfigureAwait(false));

        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount, BoundedCapacity = MaxBounded });


        var uploadBlock = new ActionBlock<FileData>(sqlC.InsertIntoDatabaseAsync, new ExecutionDataflowBlockOptions { BoundedCapacity = MaxBounded });

        //getBytesBlock.LinkTo(computeHashBlock, new DataflowLinkOptions { PropagateCompletion = true });
        computeHashBlock.LinkTo(uploadBlock, new DataflowLinkOptions { PropagateCompletion = true });

        HashSet<string> fileInDatabase = sqlC.GetAllFilePathsInDatabase();
        // Filter for images that haven't been processed.
        var newImagePaths = Directory.GetFiles(imgFolder).Where(x => !fileInDatabase.Contains(x));

        foreach (string imgPath in newImagePaths)
        {
            //await getBytesBlock.SendAsync(imgPath).ConfigureAwait(false);
            await computeHashBlock.SendAsync(imgPath).ConfigureAwait(false);
            ++totalImagesProcessed;
        }
        //getBytesBlock.Complete();
        computeHashBlock.Complete();
        await uploadBlock.Completion;

        Console.WriteLine($"\tNew Images: {totalImagesProcessed}\t");

        if (totalImagesProcessed > 0)
        {
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
                                                where file.MarkedForDeletion
                                                select file).ToList();

                sqlC.DeleteEntriesFromDatabase(filesToDelete);
                Console.WriteLine($"\tImages Deleted: {filesToDelete.Count}");
            }
        }
    }
    finally
    {
        connection.Close();
    }
}
watch.Stop();
TimeSpan time = TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds);
Console.WriteLine($"\nCompleted in {time:hh\\:mm\\:ss\\.fff}.");