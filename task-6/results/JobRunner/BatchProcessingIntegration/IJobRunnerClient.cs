namespace JobRunner.BatchProcessingIntegration;

public interface IJobRunnerClient
{
    Task RunJob(CancellationToken ct);
}