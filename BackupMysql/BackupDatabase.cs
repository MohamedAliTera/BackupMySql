using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using System.Data;
using System.Net;

namespace BackupMysql;

public class BackupDatabase
{
    private readonly ILogger _logger;

    public BackupDatabase(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<BackupDatabase>();
    }

    /// <summary>
    /// Backup the active databases using http trigger
    /// </summary>
    [Function("BackupActiveHttp")]
    public async Task<HttpResponseData> RunBackupActiveHttp([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req)
    {
        _logger.LogInformation("Started active databases backup triggerred by http");

        try
        {
            await BackupActive();

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "text/plain; charset=utf-8");
            response.WriteString("Backup completed successfully");
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError("An error occurred: {message}", ex.Message);

            var response = req.CreateResponse(HttpStatusCode.BadRequest);
            response.Headers.Add("Content-Type", "text/plain; charset=utf-8");
            response.WriteString(ex.Message);
            return response;
        }
    }

    /// <summary>
    /// Backup the active databases uusing a timer daily at 2pm UTC+3
    /// </summary>
    /// <param name="myTimer"></param>
    /// <returns></returns>
    [Function("BackupActiveTimer")]
    public async Task RunBackupActiveTiimer([TimerTrigger("0 17 * * *")] MyInfo myTimer)
    {
        _logger.LogInformation("Started daily database backup triggerred by timer");

        try
        {
            await BackupActive();

            _logger.LogInformation("C# Timer trigger function executed at: {currentTime}", DateTime.Now);
            _logger.LogInformation("Next timer schedule at: {nextTime}", myTimer.ScheduleStatus.Next);
        }
        catch (Exception ex)
        {
            _logger.LogError("An error occurred: {message}", ex.Message);
        }
    }

    /// <summary>
    /// Backup all databases monthly at 12 AM UTC+3
    /// </summary>
    [Function("BackupAllTimer")]
    public async Task RunBackupAllTimer([TimerTrigger("0 21 1 * *")] MyInfo myTimer)
    {
        _logger.LogInformation("Started monthly database backup triggerred by timer");

        try
        {
            await BackupAll();

            _logger.LogInformation("C# Timer trigger function executed at: {currentTime}", DateTime.Now);
            _logger.LogInformation("Next timer schedule at: {nextTime}", myTimer.ScheduleStatus.Next);
        }
        catch (Exception ex)
        {
            _logger.LogError("An error occurred: {message}", ex.Message);
        }
    }


    private async Task BackupActive()
    {
        string connectionString = Environment.GetEnvironmentVariable("MySqlConnection") ?? throw new NullReferenceException();
        string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage") ?? throw new NullReferenceException();
        string containerName = "databasebackups";

        var blobServiceClient = new BlobServiceClient(storageConnectionString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);

        using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync();

        using var command = connection.CreateCommand();

        List<string> databases = new List<string>
            {
              "aw","Extocare","ExtocareAutowash", "mysystemsetting"
            };

        foreach (var database in databases)
        {
            _logger.LogInformation("Database {dbName} backup started", database);

            command.CommandText = $"use `{database}`";
            command.ExecuteNonQuery();

            var backupFileName = $"db_{database}_{DateTime.UtcNow:yyyyMMddHHmmss}.sql";

            using (var backupStream = new MemoryStream())
            {
                using (var backup = new MySqlBackup(command))
                {
                    backup.ExportToMemoryStream(backupStream);
                }

                backupStream.Position = 0;
                await blobContainerClient.UploadBlobAsync(backupFileName, backupStream);

                _logger.LogInformation("Database {dbName} backup completed", database);
            }
        }
    }

    private async Task BackupAll()
    {
        string connectionString = Environment.GetEnvironmentVariable("MySqlConnection") ?? throw new NullReferenceException();
        string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage") ?? throw new NullReferenceException();
        string containerName = "databasebackups";

        var blobServiceClient = new BlobServiceClient(storageConnectionString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);

        using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync();

        using var command = connection.CreateCommand();

        DataTable databases = GetDatabaseList(command);

        foreach (DataRow dr in databases.Rows)
        {
            string db = dr[0].ToString()!;

            command.CommandText = $"use `{db}`";
            command.ExecuteNonQuery();

            var backupFileName = $"db_{DateTime.UtcNow:yyyyMMddHHmmss}.sql";

            using (var backupStream = new MemoryStream())
            {
                using (var backup = new MySqlBackup(command))
                {
                    backup.ExportToMemoryStream(backupStream);
                }

                backupStream.Position = 0;
                await blobContainerClient.UploadBlobAsync(backupFileName, backupStream);

                _logger.LogInformation("Database {dbName} backup completed", db);
            }
        }
    }

    private static DataTable GetDatabaseList(MySqlCommand command)
    {
        command.CommandText = "show databases;";
        DataTable dt = new DataTable();
        using (var dataAdapter = new MySqlDataAdapter(command))
        {
            dataAdapter.Fill(dt);
        }

        return dt;
    }
}

public class MyInfo
{
    public MyScheduleStatus ScheduleStatus { get; set; }

    public bool IsPastDue { get; set; }
}

public class MyScheduleStatus
{
    public DateTime Last { get; set; }

    public DateTime Next { get; set; }

    public DateTime LastUpdated { get; set; }
}
