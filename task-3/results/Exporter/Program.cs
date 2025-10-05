
using System.Text;
using Npgsql;

namespace Exporter;

internal class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length < 3)
        {
            throw new Exception($"Not enough arguments to run a job. Required number is 3 but received {args.Length}");
        }
        
        var connectionString = args[0];
        var targetFolderName = args[1];
        var tableNames = args[2..];
        Console.WriteLine($"Connecting to {connectionString}");
        Console.WriteLine($"Target folder: {targetFolderName}");
        Console.WriteLine($"Tables: {tableNames}");
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        
        Directory.CreateDirectory(targetFolderName);
        foreach (var tableName in tableNames)
        {
            var targetFile = Path.Combine(targetFolderName, $"{tableName}_export_{DateTime.UtcNow:yyyyMMdd}.csv");
            await ExportToCsv(conn, tableName, targetFile);
        }
    }

    private static async Task ExportToCsv(NpgsqlConnection connection, string tableName, string targetFile)
    {
        const string commandFormat = """
                               COPY (SELECT * FROM "{0}") TO STDOUT WITH (FORMAT CSV, HEADER TRUE);
                               """;
        
        var cmd = string.Format(commandFormat, tableName);
        await using var writer = new StreamWriter(targetFile, false, Encoding.UTF8);

        using var copyReader = await connection.BeginTextExportAsync(cmd);
        while (await copyReader.ReadLineAsync() is { } line)
        {
            await writer.WriteLineAsync(line);
        }
    }
}