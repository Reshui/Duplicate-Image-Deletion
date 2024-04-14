namespace SQL_Functions;
public class FileData
{
    public bool HashInDatabase { get; }
    public bool FilePathInDatabase { get; }
    public string FilePath { get; }
    public string? AlternateFileLocation;
    public bool MarkedForDeletion { get; set; }
    public byte[]? HashCode { get; set; }

    /// <param name="hashInDatabase">Boolean that represents if the file's hash is present in the database.</param>
    /// <param name="filePathInDatabase">Boolean that represents if the file name and path is found in the database.</param>
    /// <param name="filePath">File name and path.</param>
    /// <param name="hashCode">Byte array generated from a hash-algorithm.</param>
    /// <param name="alternateFileLocation">Current location of file if not equal to <paramref name="filePath"/>.</param>
    public FileData(bool hashInDatabase, bool filePathInDatabase, string filePath, byte[]? hashCode, string? alternateFileLocation = null)
    {
        HashInDatabase = hashInDatabase;
        FilePathInDatabase = filePathInDatabase;
        FilePath = filePath;
        HashCode = hashCode;
        AlternateFileLocation = alternateFileLocation;
    }
}