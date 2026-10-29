using Microsoft.AspNetCore.Server.Kestrel.Core;
using MessagingAgent.Broker;

// Doua porturi separate, pentru doua tipuri de participanti (conexiuni de retea independente):
//   5000 -> publisheri (serviciul Publisher)
//   5001 -> subscriberi (serviciul Subscriber)
const int PublisherPort = 5000;
const int SubscriberPort = 5001;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    // gRPC foloseste HTTP/2; fara TLS (conexiune simpla, ca sa mearga usor intre laptopuri)
    options.ListenAnyIP(PublisherPort, o => o.Protocols = HttpProtocols.Http2);
    options.ListenAnyIP(SubscriberPort, o => o.Protocols = HttpProtocols.Http2);
});

builder.Logging.SetMinimumLevel(LogLevel.Warning); // fara zgomotul implicit din ASP.NET Core

builder.Services.AddGrpc();
builder.Services.AddSingleton<BrokerState>();

var app = builder.Build();

// fiecare serviciu raspunde DOAR pe portul lui
app.MapGrpcService<PublisherGrpcService>().RequireHost($"*:{PublisherPort}");
app.MapGrpcService<SubscriberGrpcService>().RequireHost($"*:{SubscriberPort}");

// cream starea acum (reincarca istoricul si abonarile de pe disc inainte sa primim clienti)
app.Services.GetRequiredService<BrokerState>();

Console.WriteLine($"[Broker] gRPC: publisheri pe portul {PublisherPort}, subscriberi pe portul {SubscriberPort}.");
app.Run();
