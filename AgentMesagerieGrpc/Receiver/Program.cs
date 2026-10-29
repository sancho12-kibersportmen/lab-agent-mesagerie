using Grpc.Core;
using Grpc.Net.Client;
using MessagingAgent.Contracts;

// Receiver = SUBSCRIBER: se conecteaza la broker, primeste mesajele prin fluxul gRPC Connect si isi
// gestioneaza abonarile cu Subscribe / Unsubscribe / ListTopics.
// Daca broker-ul ruleaza pe alt laptop, schimba adresa de mai jos.
const string BrokerAddress = "http://127.0.0.1:5001"; // portul pe care broker-ul asculta subscriberii

Console.Write("Subscriber ID (ex: S1): ");
string subscriberId = (Console.ReadLine() ?? "").Trim();
if (subscriberId.Length == 0) subscriberId = "subscriber1";

using GrpcChannel channel = GrpcChannel.ForAddress(BrokerAddress);
var client = new Subscriber.SubscriberClient(channel);
using var cts = new CancellationTokenSource();

// 1) lista topicurilor existente (cele la care esti deja abonat au [abonat])
await PrintTopicsAsync();

// 2) deschidem fluxul de mesaje (server streaming). Broker-ul ne reabonaza automat la topicurile
//    salvate pentru acest ID si ne trimite istoricul lor, apoi mesajele noi in timp real.
AsyncServerStreamingCall<ChatMessage> stream =
    client.Connect(new ConnectRequest { SubscriberId = subscriberId }, cancellationToken: cts.Token);
Task receiveTask = ReceiveLoopAsync(stream);

Console.WriteLine("Comenzi: topics | sub <topic> | unsub <topic> | exit\n");

while (true)
{
    string input = (Console.ReadLine() ?? "exit").Trim();
    if (input.Equals("exit", StringComparison.OrdinalIgnoreCase)) break;

    if (receiveTask.IsCompleted)
    {
        Console.WriteLine("[Subscriber] Conexiunea cu broker-ul s-a pierdut. Reporneste Receiver-ul.");
        break;
    }

    if (input.Equals("topics", StringComparison.OrdinalIgnoreCase))
    {
        await PrintTopicsAsync();
        continue;
    }

    string[] parts = input.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    if (parts.Length != 2) continue;

    try
    {
        StatusReply reply;
        if (parts[0].Equals("sub", StringComparison.OrdinalIgnoreCase))
            reply = await client.SubscribeAsync(new TopicRequest { SubscriberId = subscriberId, Topic = parts[1] });
        else if (parts[0].Equals("unsub", StringComparison.OrdinalIgnoreCase))
            reply = await client.UnsubscribeAsync(new TopicRequest { SubscriberId = subscriberId, Topic = parts[1] });
        else
            continue;

        if (!reply.Ok) Console.WriteLine($"[Subscriber] Respins de broker: {reply.Error}");
    }
    catch (RpcException ex)
    {
        Console.WriteLine($"[Subscriber] Broker-ul nu raspunde ({ex.StatusCode}).");
    }
}

cts.Cancel(); // inchide fluxul; broker-ul vede deconectarea
Console.WriteLine("[Subscriber] Deconectat.");

// ---------------------------------------------------------------------------------------------

async Task ReceiveLoopAsync(AsyncServerStreamingCall<ChatMessage> call)
{
    try
    {
        await foreach (ChatMessage message in call.ResponseStream.ReadAllAsync(cts.Token))
        {
            Console.WriteLine(
                $"[{message.Timestamp.ToDateTime().ToLocalTime():HH:mm:ss}] [{message.Topic}] {message.SenderId}: {message.Content}");
        }
        Console.WriteLine("[Subscriber] Broker-ul a inchis fluxul.");
    }
    catch (OperationCanceledException)
    {
        // inchidere normala (exit)
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled)
    {
        // inchidere normala (exit)
    }
    catch (RpcException ex)
    {
        Console.WriteLine($"[Subscriber] Conexiunea cu broker-ul s-a pierdut ({ex.StatusCode}).");
    }
}

async Task PrintTopicsAsync()
{
    try
    {
        ListTopicsReply reply = await client.ListTopicsAsync(
            new ListTopicsRequest { SubscriberId = subscriberId },
            deadline: DateTime.UtcNow.AddSeconds(5));

        Console.WriteLine("--- Topicuri disponibile ---");
        if (reply.Topics.Count == 0) Console.WriteLine("(niciun topic inca)");
        foreach (TopicInfo topic in reply.Topics)
        {
            Console.WriteLine($"{topic.Name} ({topic.Messages} mesaje, {topic.Subscribers} abonati){(topic.Subscribed ? " [abonat]" : "")}");
        }
        Console.WriteLine("----------------------------");
    }
    catch (RpcException ex)
    {
        Console.WriteLine($"[Subscriber] Nu pot citi lista de topicuri ({ex.StatusCode}). Ruleaza broker-ul pe {BrokerAddress}?");
    }
}
