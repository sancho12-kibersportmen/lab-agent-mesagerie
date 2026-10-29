using Grpc.Core;
using Grpc.Net.Client;
using MessagingAgent.Contracts;

// Sender = PUBLISHER: publica mesaje pe un topic prin apelul gRPC Publish.
// Nu stie cine sunt abonatii. Daca broker-ul ruleaza pe alt laptop, schimba adresa de mai jos.
const string BrokerAddress = "http://127.0.0.1:5000"; // portul pe care broker-ul asculta publisherii

Console.Write("Publisher ID (numele tau): ");
string senderId = (Console.ReadLine() ?? "").Trim();
if (senderId.Length == 0) senderId = "publisher1";

// canalul gRPC (HTTP/2) catre broker; clientul generat din .proto ("stub") ascunde socketurile si serializarea
using GrpcChannel channel = GrpcChannel.ForAddress(BrokerAddress);
var client = new Publisher.PublisherClient(channel);
Console.WriteLine($"[Publisher] Configurat pentru broker-ul de la {BrokerAddress}.");
Console.WriteLine("Scrie 'exit' in loc de topic pentru a inchide. Topic gol = ultimul topic folosit.\n");

string lastTopic = "";
while (true)
{
    Console.Write("Topic: ");
    string topic = (Console.ReadLine() ?? "exit").Trim();
    if (topic.Equals("exit", StringComparison.OrdinalIgnoreCase)) break;
    if (topic.Length == 0) topic = lastTopic;
    if (topic.Length == 0) continue;
    lastTopic = topic;

    Console.Write("Mesaj: ");
    string content = Console.ReadLine() ?? "";

    try
    {
        // apel de procedura la distanta: arata ca o metoda locala, dar ruleaza pe broker
        PublishReply reply = await client.PublishAsync(
            new PublishRequest { SenderId = senderId, Topic = topic, Content = content },
            deadline: DateTime.UtcNow.AddSeconds(5));

        if (reply.Accepted)
            Console.WriteLine($"[Publisher] Publicat pe '{topic}' (id {reply.MessageId}): {reply.Subscribers} abonati, {reply.Delivered} online acum.\n");
        else
            Console.WriteLine($"[Publisher] Respins de broker: {reply.Error}\n");
    }
    catch (RpcException ex)
    {
        Console.WriteLine($"[Publisher] Broker-ul nu raspunde ({ex.StatusCode}). Verifica daca ruleaza si adresa {BrokerAddress}.\n");
    }
}

Console.WriteLine("[Publisher] Deconectat.");
