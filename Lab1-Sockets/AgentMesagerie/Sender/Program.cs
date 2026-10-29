using System.Net.Sockets;
using System.Text.Json;

namespace MessagingAgent.Sender
{
    // Sender = PUBLISHER: publica mesaje pe un topic; nu stie cine sunt abonatii.
    internal class Program
    {
        private const string BrokerIp = "127.0.0.1"; // adresa broker-ului (schimbati daca ruleaza pe alt PC)
        private const int BrokerPort = 5000;         // portul pe care broker-ul asculta publisherii

        private static async Task Main()
        {
            Console.Write("Publisher ID (numele tau): ");
            string senderId = (Console.ReadLine() ?? "").Trim();
            if (senderId.Length == 0) senderId = "publisher1";

            using var client = new TcpClient();
            await client.ConnectAsync(BrokerIp, BrokerPort);
            Console.WriteLine("[Publisher] Conectat la broker.");

            var writer = new StreamWriter(client.GetStream()) { AutoFlush = true };

            Console.WriteLine("Scrie 'exit' in loc de topic pentru a inchide. Topic gol = ultimul topic folosit.\n");

            string lastTopic = "";
            while (true)
            {
                Console.Write("Topic: ");
                string topic = (Console.ReadLine() ?? "exit").Trim();
                if (string.Equals(topic, "exit", StringComparison.OrdinalIgnoreCase)) break;
                if (topic.Length == 0) topic = lastTopic;
                if (topic.Length == 0) continue;
                lastTopic = topic;

                Console.Write("Mesaj: ");
                string content = Console.ReadLine() ?? "";

                var message = new ChatMessage
                {
                    Type = MessageType.Publish,
                    SenderId = senderId,
                    Topic = topic,
                    Content = content
                };

                try
                {
                    await writer.WriteLineAsync(JsonSerializer.Serialize(message));
                    Console.WriteLine($"[Publisher] Publicat pe topicul '{topic}'.\n");
                }
                catch (IOException)
                {
                    Console.WriteLine("[Publisher] Broker-ul nu mai raspunde. Inchid.");
                    break;
                }
            }

            Console.WriteLine("[Publisher] Deconectat.");
        }
    }
}
