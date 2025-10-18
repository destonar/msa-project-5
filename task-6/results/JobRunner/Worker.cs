using JobRunner.BatchProcessingIntegration;

namespace JobRunner;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IJobRunnerClient _jobRunnerClient;

    public Worker(ILogger<Worker> logger, IJobRunnerClient jobRunnerClient)
    {
        _logger = logger;
        _jobRunnerClient = jobRunnerClient;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
        await Task.Delay(5000, stoppingToken);
        
        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Running job...");

            try
            {
                await _jobRunnerClient.RunJob(stoppingToken);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Failed to run job");
            }
            finally
            {
                await Task.Delay(5000, stoppingToken);
            }
        }
    }
}