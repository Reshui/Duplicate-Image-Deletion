namespace SQL_Functions;
using Microsoft.Data.Sqlite;
using SQLitePCL;

public class SQL_Connector
{
    public string HashTableName;
    public string DatabaseName;
    public SqliteConnection Connection;
    private readonly int _useOriginal = 0;
    private readonly int _useAlternate = 1;
    public int CompletionCount = 0;

    //public static hashAlgo = new SHA512();

    public SQL_Connector(SqliteConnection connection, string hashTableName, string databaseName)
    {
        HashTableName = hashTableName;
        DatabaseName = databaseName;

        Connection = connection;

        using (var command = Connection.CreateCommand())
        {
            command.CommandText =
            $@"CREATE TABLE IF NOT EXISTS {hashTableName}
            (FileName Text PRIMARY KEY, AlternatePath Text,AlternatePathActive INTEGER NOT NULL,
            HashCode BLOB NOT NULL);";

            command.ExecuteNonQuery();

            command.CommandText = @"PRAGMA journal_mode = 'wal'";
            command.ExecuteNonQuery();
        }
    }
    public int ReturnNumberOfRecords()
    {
        using (var command = Connection.CreateCommand())
        {
            command.CommandText = $"SELECT COUNT(*) FROM {HashTableName};";

            return Convert.ToInt32(command.ExecuteScalar());
        }
    }
    public FileData DoesHashExistAsync(byte[] hashCode, string imgPath)
    {
        bool imgInDatabase = false;
        bool hashFound = false;
        var command = Connection.CreateCommand();

        command.CommandText =
        $@"
            SELECT FileName, AlternatePath FROM {HashTableName}
            WHERE HashCode = $TestHash;";

        command.Parameters.AddWithValue("$TestHash", hashCode);

        using var reader = command.ExecuteReader();

        if (reader.HasRows)
        {
            string? currentFileName;
            hashFound = true;
            string originalFileName;

            while (reader.Read())
            {   // Now determine if the file path/HashCode is already in the database
                try
                {
                    originalFileName = reader.GetString(0);
                }
                catch (NullReferenceException)
                {
                    Console.WriteLine($"Error attempting to get original file name from database: {imgPath}");
                    throw;
                }

                try
                {
                    if (!reader.IsDBNull(1))
                    {
                        currentFileName = reader.GetString(1);
                    }
                    else
                    {
                        currentFileName = null;
                    }
                }
                catch (NullReferenceException)
                {
                    Console.WriteLine($"Error attempting to get alternate file path from database: {imgPath}");
                    throw;
                }

                if (imgPath == originalFileName || imgPath == currentFileName)
                {
                    imgInDatabase = true;
                    break;
                }
            }
        }
        return new FileData(hashFound, imgInDatabase, imgPath, hashCode);
    }

    /// <summary>Queries the database for a list of file names currently in the database.</summary>
    /// <returns>Returns a <see cref="HashSet{string}"/> of file names.</returns>
    public HashSet<string> GetAllFilePathsInDatabase()
    {
        var filePaths = new HashSet<string>();

        using (var command = Connection.CreateCommand())
        {
            command.CommandText = @$"SELECT FileName FROM {HashTableName};";

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    string oPath = reader.GetString(0);
                    filePaths.Add(oPath);
                }
            }
        }
        return filePaths;
    }

    /// <summary>Queries the database for a list of images with duplicate hash codes.</summary>
    /// <returns> A dictionary keyed to an image's hash code containing a list of duplicate images.</returns>
    public Dictionary<byte[], List<FileData>> FilesWithDuplicates()
    {
        var command = Connection.CreateCommand();

        command.CommandText =
        @$"SELECT FileName,HashCode,AlternatePath FROM {HashTableName} as MainTable
        WHERE HashCode IN
            (SELECT HashCode FROM {HashTableName}
            GROUP BY HashCode
            HAVING COUNT(HashCode) > 1);";
        //command.Parameters.AddWithValue("$useOriginal", _useOriginal);

        var wantedFiles = new Dictionary<byte[], List<FileData>>(new ByteArrayEquality());

        using (var reader = command.ExecuteReader())
        {
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    string filePath = reader.GetString(0);
                    byte[] byteCode = (byte[])reader.GetValue(1);

                    string? alternatePath = !reader.IsDBNull(2) ? reader.GetString(2) : null;

                    var imgfile = new FileData(true, true, filePath, byteCode, alternatePath);

                    if (!wantedFiles.ContainsKey(byteCode))
                    {
                        wantedFiles.Add(byteCode, new List<FileData> { imgfile });
                    }
                    else
                    {
                        wantedFiles[byteCode].Add(imgfile);
                    }

                }
            }
        }
        return wantedFiles;
    }

    /// <summary>Adds identifying information to the database using selected properties of an inputted FileData class.</summary>
    /// <param name = "fileDetails">Collection of FileData objects that contain info for new entries to be added.</param>
    public void BulkInsertToDatabase(IEnumerable<FileData> fileDetails)
    {
        using var transaction = Connection.BeginTransaction();
        using var command = Connection.CreateCommand();

        command.CommandText =
        @$"INSERT INTO {HashTableName} (FileName, AlternatePathActive, HashCode)
                VALUES ($filePath, $alternatePathValid, $byteArray);";

        var fileNameParam = command.CreateParameter();
        var hashCodeParam = command.CreateParameter();

        fileNameParam.ParameterName = "$filePath";
        hashCodeParam.ParameterName = "$byteArray";

        command.Parameters.AddWithValue("$alternatePathValid", _useOriginal);
        command.Parameters.Add(fileNameParam);
        command.Parameters.Add(hashCodeParam);

        foreach (FileData imgfile in fileDetails)
        {
            fileNameParam.Value = imgfile.FilePath;
            hashCodeParam.Value = imgfile.HashCode;

            try
            {
                command.ExecuteNonQueryAsync();
            }
            catch (SqliteException)
            {
                Console.WriteLine(imgfile.FilePath);
                throw;
            }
        }
        transaction.Commit();
    }

    public void UpdateAlternatePath(List<FileData> fileDetails)
    {

        using (var transaction = Connection.BeginTransaction())
        {
            using (var command = Connection.CreateCommand())
            {
                command.CommandText =
                @$"Update {HashTableName}
                SET AlternatePath = $newPath, AlternatePathActive = $useAlternate
                WHERE FileName = $originalPath;";

                var newAlternatePathParam = command.CreateParameter();
                var originalPathParam = command.CreateParameter();
                var useAlternateParam = command.CreateParameter();

                useAlternateParam.ParameterName = "$useAlternate";
                originalPathParam.ParameterName = "$originalPath";
                newAlternatePathParam.ParameterName = "$newPath";

                command.Parameters.Add(useAlternateParam);
                command.Parameters.Add(originalPathParam);
                command.Parameters.Add(newAlternatePathParam);

                foreach (FileData imgfile in fileDetails)
                {
                    useAlternateParam.Value = (imgfile.AlternateFileLocation == null) ? _useOriginal : _useAlternate;
                    originalPathParam.Value = imgfile.FilePath;
                    newAlternatePathParam.Value = imgfile.AlternateFileLocation;

                    command.ExecuteNonQuery();
                }
            }

            transaction.Commit();
        }

    }

    /// <summary>Deletes entries in database that match a given file path in a given list.</summary>
    /// <param name="fileDetails">A list of FileData objects that contain information for entries to be deleted from the database.</param> 
    public void DeleteEntriesFromDatabase(List<FileData> fileDetails)
    {
        using (var transaction = Connection.BeginTransaction())
        {
            using (var command = Connection.CreateCommand())
            {
                command.CommandText =
                @$"DELETE FROM {HashTableName}
                WHERE FileName = $originalPath;";

                var originalPathParam = command.CreateParameter();
                originalPathParam.ParameterName = "$originalPath";

                command.Parameters.Add(originalPathParam);

                foreach (FileData file in fileDetails)
                {
                    originalPathParam.Value = file.FilePath;
                    command.ExecuteNonQuery();
                }
            }
            transaction.Commit();
        }
    }
}
class ByteArrayEquality : IEqualityComparer<byte[]>
{
    public bool Equals(byte[]? b1, byte[]? b2)
    {
        if (b1 == null || b2 == null)
        {
            return b1 == b2;
        }
        else if (ReferenceEquals(b1, b2))
        {
            return true;
        }
        else if (b1.Length != b2.Length)
        {
            return false;
        }
        return b1.SequenceEqual(b2);

    }

    public int GetHashCode(byte[] key)
    {
        // dismisses unequal length keys and forces Equals otherwise
        return key.Length;//key.Sum(k => k);
    }
}

public class FileData
{
    public bool HashInDatabase = false;
    public bool FilePathInDatabase = false;
    public string FilePath = string.Empty;
    public string? AlternateFileLocation = null;
    public bool QueueForUpload = false;
    public bool MarkedForDeletion = false;
    public byte[]? HashCode = null;
    public byte[]? RawBytes = null;

    /// <param name="hashInDatabase">Boolean that represents if the file's hash is present in the database.</param>
    /// <param name="filePathInDatabase">Boolean that represents if the file name and path is found in the database.</param>
    /// <param name="filePath">File name and path.</param>
    /// <param name="hashCode">Byte array generated from a hash-algorithm.</param>
    /// <param name="alternateFileLocation">Current location of file if not equal to <paramref name="filePath"/>.</param>
    public FileData(bool hashInDatabase, bool filePathInDatabase, string filePath, byte[]? hashCode, string? alternateFileLocation = null, byte[]? rawBytes = null)
    {
        HashInDatabase = hashInDatabase;
        FilePathInDatabase = filePathInDatabase;
        FilePath = filePath;
        HashCode = hashCode;
        RawBytes = rawBytes;
        AlternateFileLocation = alternateFileLocation;

        if (!FilePathInDatabase) QueueForUpload = true;
    }
}