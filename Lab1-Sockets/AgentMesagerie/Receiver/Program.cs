using System.Net.Sockets;
using System.Text.Json;

namespace MessagingAgent.Receiver
{
    // Receiver = SUBSCRIBER: se aboneaza la unul sau mai multe topicuri si primeste mesajele lor.
    internal class Program
    {
        private const string BrokerIp = "127.0.0.1"; // adresa broker-ului (schimbati daca ruleaza pe alt PC)
        private const int BrokerPort = 5001;         // portul pe care broker-ul asculta subscriberii

        private static async Task Main()
        {
            Console.Write("Subscriber ID (ex: S1): ");
            string subscriberId = (Console.ReadLine() ?? "").Trim();
            if (subscriberId.Length == 0) subscriberId = "subscriber1";

            using var client = new TcpClient();
            await client.ConnectAsync(BrokerIp, BrokerPort);
            Console.WriteLine("[Subscriber] Conectat la broker.");

            NetworkStream stream = client.GetStream();
            var writer = new StreamWriter(stream) { AutoFlush = true };
            var reader = new StreamReader(stream);

            // pornim citirea mesajelor inainte de a ne identifica, ca sa afisam tot ce trimite broker-ul
            Task receiveTask = ReceiveLoopAsync(reader);

            // ne identificam la broker (prima cerere = lista topicurilor). Broker-ul raspunde cu
            // topicurile existente si, daca acest ID a mai fost conectat, ne reabonaza automat la
            // topicurile lui si ne retrimite istoricul lor.
            await SendCommandAsync(writer, MessageType.ListTopics, subscriberId, "");

            Console.WriteLine("Comenzi: topics | sub <topic> | unsub <topic> | exit\n");

            // bucla de comenzi: permite abonarea la topicuri noi fara sa repornim programul
            while (true)
            {
                string input = (Console.ReadLine() ?? "exit").Trim();
                if (string.Equals(input, "exit", StringComparison.OrdinalIgnoreCase)) break;
                if (receiveTask.IsCompleted) break;

                if (input.Equals("topics", StringComparison.OrdinalIgnoreCase))
                {
                    try { await SendCommandAsync(writer, MessageType.ListTopics, subscriberId, ""); }
                    catch (IOException)
                    {
                        Console.WriteLine("[Subscriber] Broker-ul nu mai raspunde.");
                        break;
                    }
                    continue;
                }

                string[] parts = input.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                if (parts.Length != 2) continue;

                try
                {
                    if (parts[0].Equals("sub", StringComparison.OrdinalIgnoreCase))
                        await SendCommandAsync(writer, MessageType.Subscribe, subscriberId, parts[1]);
                    else if (parts[0].Equals("unsub", StringComparison.OrdinalIgnoreCase))
                        await SendCommandAsync(writer, MessageType.Unsubscribe, subscriberId, parts[1]);
                }
                catch (IOException)
                {
                    Console.WriteLine("[Subscriber] Broker-ul nu mai raspunde.");
                    break;
                }
            }

            Console.WriteLine("[Subscriber] Deconectat.");
        }

        private static Task SendCommandAsync(StreamWriter writer, MessageType type, string subscriberId, string topic)
        {
            var command = new ChatMessage { Type = type, SubscriberId = subscriberId, Topic = topic };
            return writer.WriteLineAsync(JsonSerializer.Serialize(command));
        }

        private static async Task ReceiveLoopAsync(StreamReader reader)
        {
            try
            {
                while (true)
                {
                    string? line = await reader.ReadLineAsync();
                    if (line == null)
                    {
                        Console.WriteLine("[Subscriber] Broker-ul a inchis conexiunea.");
                        return;
                    }

                    try
                    {
                        var message = JsonSerializer.Deserialize<ChatMessage>(line);
                        if (message != null && message.Type == MessageType.Delivery)
                        {
                            Console.WriteLine(
                                $"[{message.Timestamp.ToLocalTime():HH:mm:ss}] [{message.Topic}] {message.SenderId}: {message.Content}");
                        }
                        else if (message != null && message.Type == MessageType.TopicList)
                        {
                            Console.WriteLine("--- Topicuri disponibile ---");
                            Console.WriteLine(string.IsNullOrEmpty(message.Content)
                                ? "(niciun topic inca)"
                                : message.Content);
                            Console.WriteLine("----------------------------");
                        }
                    }
                    catch (JsonException)
                    {
                        Console.WriteLine("[Subscriber] Mesaj invalid primit, ignorat.");
                    }
                }
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException)
            {
                Console.WriteLine("[Subscriber] Conexiunea cu broker-ul s-a pierdut.");
            }
        }
    }
}
