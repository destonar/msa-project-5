using System.Net.Http.Headers;
using System.Net.Mime;
using JobRunner;
using JobRunner.BatchProcessingIntegration;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.AddOpenTelemetry(logging =>
{
    logging.IncludeFormattedMessage = true;
    logging.IncludeScopes = true;
});
        
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(builder.Environment.ApplicationName))
    .WithTracing(b => b
        .AddHttpClientInstrumentation()
        .AddOtlpExporter(o => o.Endpoint = new Uri("http://otel-collector:4317"))
    );

var opts = new JobRunnerOpts();
builder.Configuration.GetSection(nameof(JobRunnerOpts)).Bind(opts);
builder.Services.AddHttpClient<IJobRunnerClient, JobRunnerClient>()
    .ConfigureHttpClient(client =>
    {
        client.BaseAddress = new Uri(opts.BaseAddress);
        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue(MediaTypeNames.Application.Json));
    })
    .ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
    {
        ServerCertificateCustomValidationCallback = (_, _, _, _) => true
    });

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();