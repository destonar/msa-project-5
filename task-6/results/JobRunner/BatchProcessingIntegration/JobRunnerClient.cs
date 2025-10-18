namespace JobRunner.BatchProcessingIntegration;

public class JobRunnerClient : IJobRunnerClient
{
    private readonly HttpClient _httpClient;

    public JobRunnerClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task RunJob(CancellationToken ct)
    {
        var response = await _httpClient.PostAsync("start_processing", null, ct);
        response.EnsureSuccessStatusCode();
    }
}