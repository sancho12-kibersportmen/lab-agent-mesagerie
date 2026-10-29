using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Channels;

namespace MessagingAgent.Broker
{
    // Un topic = un canal logic. Pastreaza istoricul complet al mesajelor si lista
    // subscriberilor abonati (lista ramane si cat timp un subscriber e offline).
    internal class TopicState
    {
        public readonly object Lock = new();
        public readonly List<ChatMessage> History = new();
        public readonly HashSet<string> Subscribers = new();
    }

    // Conexiunea live a unui subscriber. Mai multe fire pot scrie simultan pe acelasi socket
    // (replay + mesaje noi), de aceea scrierea e protejata cu un semafor.
    internal class SubscriberConnection
    {
        public string SubscriberId { get; }
        private readonly StreamWriter _writer;
        public SemaphoreSlim WriteLock { get; } = new(1, 1);

        // id-urile mesajelor deja trimise prin replay; un mesaj live cu acelasi id e sarit, ca sa nu
        // apara de doua ori daca a fost publicat chiar in timpul reconectarii (accesat doar sub WriteLock)
        public HashSet<string> ReplayedIds { get; } = new();

        public SubscriberConnection(string subscriberId, StreamWriter writer)
        {
            SubscriberId = subscriberId;
            _writer = writer;
        }

        public async Task SendAsync(ChatMessage message)
        {
            await WriteLock.WaitAsync();
            try
            {
                if (ReplayedIds.Contains(message.Id)) return;
                await SendUnlockedAsync(message);
            }
            finally { WriteLock.Release(); }
        }

        // apelantul trebuie sa detina deja WriteLock
        public Task SendUnlockedAsync(ChatMessage message) =>
            _writer.WriteLineAsync(JsonSerializer.Serialize(message));
    }

    internal class FailedMessage
    {
        public ChatMessage Message { get; set; } = new();
        public string TargetSubscriberId { get; set; } = string.Empty;
        public string Reason { get; set; } = string.Empty;
        public DateTime FailedAt { get; set; } = DateTime.UtcNow;
    }

    internal class Program
    {
        // porturi separate pentru publisheri si subscriberi (conexiuni de retea independente)
        private const int PublisherPort = 5000;
        private const int SubscriberPort = 5001;
        private const string FailedMessagesFile = "failed_messages.json";

        // PERSISTENTA: istoricul mesajelor (cate un fisier .log per topic, un JSON pe linie) si
        // abonarile (subscriptions.json) se salveaza pe disc si se reincarca la pornirea brokerului
        private const string DataDirectory = "broker-data";
        private static readonly string SubscriptionsFile = Path.Combine(DataDirectory, "subscriptions.json");
        private static readonly object _subsFileLock = new();

        // coada de scriere asincrona: PublishAsync pune mesajul in coada (instant), iar un singur
        // task de fundal il scrie pe disc -> publisherii nu asteapta dupa operatiile pe fisiere
        private static readonly Channel<ChatMessage> _persistQueue =
            Channel.CreateUnbounded<ChatMessage>(new UnboundedChannelOptions { SingleReader = true });

        // numele topicului -> starea lui (istoric + abonati)
        private static readonly ConcurrentDictionary<string, TopicState> _topics =
            new(StringComparer.OrdinalIgnoreCase);

        // subscriberId -> conexiunea curenta (exista doar cat timp subscriber-ul e online)
        private static readonly ConcurrentDictionary<string, SubscriberConnection> _connected = new();

        private static readonly object _failedFileLock = new();
        private static readonly JsonSerializerOptions _indented = new() { WriteIndented = true };

        private static async Task Main()
        {
            EnsureFailedMessagesFileExists();
            LoadPersistedState();
            _ = Task.Run(PersistWriterAsync);
            Console.WriteLine($"[Broker] Date persistente: {Path.GetFullPath(DataDirectory)}");
            Console.WriteLine($"[Broker] Mesaje esuate: {Path.GetFullPath(FailedMessagesFile)}");

            var publisherListener = new TcpListener(IPAddress.Any, PublisherPort);
            var subscriberListener = new TcpListener(IPAddress.Any, SubscriberPort);
            publisherListener.Start();
            subscriberListener.Start();
            Console.WriteLine($"[Broker] Publisheri pe portul {PublisherPort}, subscriberi pe portul {SubscriberPort}...");

            await Task.WhenAll(
                AcceptLoopAsync(publisherListener, HandlePublisherAsync),
                AcceptLoopAsync(subscriberListener, HandleSubscriberAsync));
        }

        // fiecare conexiune noua e tratata pe un Task separat -> procesare concurenta
        private static async Task AcceptLoopAsync(TcpListener listener, Func<TcpClient, Task> handler)
        {
            while (true)
            {
                TcpClient client = await listener.AcceptTcpClientAsync();
                _ = Task.Run(() => handler(client));
            }
        }

        // ---------------------------------------------------------------- PUBLISHER

        private static async Task HandlePublisherAsync(TcpClient client)
        {
            string? remote = client.Client.RemoteEndPoint?.ToString();
            Console.WriteLine($"[Broker] Publisher conectat: {remote}");
            try
            {
                var reader = new StreamReader(client.GetStream());
                while (true)
                {
                    string? line = await reader.ReadLineAsync();
                    if (line == null) break;

                    ChatMessage? message;
                    try { message = JsonSerializer.Deserialize<ChatMessage>(line); }
                    catch (JsonException)
                    {
                        // mesaj invalid -> ignorat, broker-ul nu cade
                        Console.WriteLine($"[Broker] Mesaj invalid (JSON corupt) de la {remote}, ignorat.");
                        continue;
                    }

                    if (message == null || message.Type != MessageType.Publish) continue;
                    await PublishAsync(message);
                }
            }
            catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
            {
                // conexiunea publisher-ului a picat brusc
            }
            finally
            {
                Console.WriteLine($"[Broker] Publisher deconectat: {remote}");
                client.Close();
            }
        }

        private static async Task PublishAsync(ChatMessage message)
        {
            if (string.IsNullOrWhiteSpace(message.Topic) || string.IsNullOrWhiteSpace(message.Content))
            {
                Console.WriteLine("[Broker] Mesaj respins: topic sau continut gol.");
                AppendFailed(message, "(niciunul)", "mesaj invalid: topic sau continut gol");
                return;
            }

            message.Topic = message.Topic.Trim();
            message.Type = MessageType.Delivery; // tipul cu care il primesc subscriberii

            TopicState topic = _topics.GetOrAdd(message.Topic, _ => new TopicState());

            // adaugarea in istoric si citirea listei de abonati se fac atomic: un subscriber
            // care se aboneaza simultan primeste mesajul fie prin replay, fie live - niciodata dublu
            List<string> targets;
            lock (topic.Lock)
            {
                topic.History.Add(message);
                _persistQueue.Writer.TryWrite(message); // salvare pe disc (asincron, in fundal)
                targets = topic.Subscribers.ToList();
            }

            Console.WriteLine($"[Broker] [{message.Topic}] mesaj {message.Id} de la '{message.SenderId}' ({targets.Count} abonati)");

            if (targets.Count == 0)
            {
                AppendFailed(message, "(niciunul)", "niciun subscriber abonat la topic");
                return;
            }

            await Task.WhenAll(targets.Select(id => DeliverAsync(id, message)));
        }

        private static async Task DeliverAsync(string subscriberId, ChatMessage message)
        {
            if (!_connected.TryGetValue(subscriberId, out var conn))
            {
                Console.WriteLine($"[Broker] '{subscriberId}' offline -> mesaj {message.Id} salvat ca esuat (il va primi la reconectare).");
                AppendFailed(message, subscriberId, "subscriber offline");
                return;
            }

            try
            {
                await conn.SendAsync(message);
            }
            catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
            {
                _connected.TryRemove(new KeyValuePair<string, SubscriberConnection>(subscriberId, conn));
                Console.WriteLine($"[Broker] Livrare esuata catre '{subscriberId}': {ex.Message}");
                AppendFailed(message, subscriberId, "eroare de retea la livrare: " + ex.Message);
            }
        }

        // --------------------------------------------------------------- SUBSCRIBER

        private static async Task HandleSubscriberAsync(TcpClient client)
        {
            SubscriberConnection? conn = null;
            try
            {
                NetworkStream stream = client.GetStream();
                var reader = new StreamReader(stream);
                var writer = new StreamWriter(stream) { AutoFlush = true };

                while (true)
                {
                    string? line = await reader.ReadLineAsync();
                    if (line == null) break;

                    ChatMessage? message;
                    try { message = JsonSerializer.Deserialize<ChatMessage>(line); }
                    catch (JsonException)
                    {
                        Console.WriteLine("[Broker] Mesaj invalid de la un subscriber, ignorat.");
                        continue;
                    }
                    if (message == null || string.IsNullOrWhiteSpace(message.SubscriberId)) continue;

                    // primul mesaj valid identifica subscriber-ul pentru intreaga conexiune
                    if (conn == null)
                    {
                        conn = new SubscriberConnection(message.SubscriberId.Trim(), writer);
                        await WelcomeAsync(conn); // inregistrare + lista topicuri + reabonare automata
                        if (message.Type == MessageType.ListTopics) continue; // lista a fost deja trimisa
                    }

                    if (message.Type == MessageType.Subscribe)
                        await SubscribeAsync(conn, message.Topic);
                    else if (message.Type == MessageType.Unsubscribe)
                        Unsubscribe(conn, message.Topic);
                    else if (message.Type == MessageType.ListTopics)
                        await SendTopicListAsync(conn);
                }
            }
            catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
            {
                // conexiunea subscriber-ului a picat brusc
            }
            finally
            {
                if (conn != null)
                {
                    // scoatem intrarea doar daca nu a fost deja inlocuita de o reconectare
                    _connected.TryRemove(new KeyValuePair<string, SubscriberConnection>(conn.SubscriberId, conn));
                    Console.WriteLine($"[Broker] Subscriber '{conn.SubscriberId}' deconectat.");
                }
                client.Close();
            }
        }

        private static async Task SubscribeAsync(SubscriberConnection conn, string topicName)
        {
            if (string.IsNullOrWhiteSpace(topicName)) return;
            topicName = topicName.Trim();
            TopicState topic = _topics.GetOrAdd(topicName, _ => new TopicState());

            // tinem semaforul cat dureaza replay-ul, ca mesajele noi sa astepte dupa istoric (ordine corecta)
            await conn.WriteLock.WaitAsync();
            try
            {
                List<ChatMessage> snapshot;
                lock (topic.Lock)
                {
                    topic.Subscribers.Add(conn.SubscriberId);
                    snapshot = new List<ChatMessage>(topic.History);
                }
                SaveSubscriptions();

                Console.WriteLine($"[Broker] '{conn.SubscriberId}' abonat la '{topicName}' - trimit istoricul ({snapshot.Count} mesaje).");
                foreach (var message in snapshot)
                {
                    conn.ReplayedIds.Add(message.Id);
                    await conn.SendUnlockedAsync(message);
                }
            }
            finally
            {
                conn.WriteLock.Release();
            }
        }

        // La conectarea unui subscriber (prima lui cerere): il inregistram, ii trimitem lista topicurilor
        // existente si, daca acest ID a mai fost abonat (abonarile sunt salvate pe disc), il reabonam
        // automat la topicurile lui si ii retrimitem tot istoricul, fara sa se aboneze manual din nou.
        private static async Task WelcomeAsync(SubscriberConnection conn)
        {
            // tinem semaforul tot timpul: mesajele noi asteapta dupa lista si dupa istoric (ordine corecta)
            await conn.WriteLock.WaitAsync();
            try
            {
                _connected[conn.SubscriberId] = conn; // inlocuieste o eventuala conexiune veche
                Console.WriteLine($"[Broker] Subscriber '{conn.SubscriberId}' conectat.");

                await conn.SendUnlockedAsync(BuildTopicListMessage(conn.SubscriberId));

                foreach (var entry in _topics.OrderBy(t => t.Key, StringComparer.OrdinalIgnoreCase))
                {
                    List<ChatMessage> snapshot;
                    lock (entry.Value.Lock)
                    {
                        if (!entry.Value.Subscribers.Contains(conn.SubscriberId)) continue;
                        snapshot = new List<ChatMessage>(entry.Value.History);
                    }

                    Console.WriteLine($"[Broker] '{conn.SubscriberId}' reabonat automat la '{entry.Key}' - trimit istoricul ({snapshot.Count} mesaje).");
                    foreach (var message in snapshot)
                    {
                        conn.ReplayedIds.Add(message.Id);
                        await conn.SendUnlockedAsync(message);
                    }
                }
            }
            finally
            {
                conn.WriteLock.Release();
            }
        }

        // Lista topicurilor existente (nume, nr. mesaje, nr. abonati, [abonat] daca subscriber-ul e deja abonat).
        // Lista merge in campul Content, cate un topic pe linie.
        private static ChatMessage BuildTopicListMessage(string subscriberId)
        {
            var lines = new List<string>();
            foreach (var entry in _topics.OrderBy(t => t.Key, StringComparer.OrdinalIgnoreCase))
            {
                int messages, subscribers;
                bool subscribed;
                lock (entry.Value.Lock)
                {
                    messages = entry.Value.History.Count;
                    subscribers = entry.Value.Subscribers.Count;
                    subscribed = entry.Value.Subscribers.Contains(subscriberId);
                }
                lines.Add($"{entry.Key} ({messages} mesaje, {subscribers} abonati){(subscribed ? " [abonat]" : "")}");
            }

            return new ChatMessage
            {
                Type = MessageType.TopicList,
                SenderId = "broker",
                Content = string.Join("\n", lines)
            };
        }

        private static async Task SendTopicListAsync(SubscriberConnection conn)
        {
            await conn.SendAsync(BuildTopicListMessage(conn.SubscriberId));
            Console.WriteLine($"[Broker] Lista de topicuri trimisa catre '{conn.SubscriberId}'.");
        }

        private static void Unsubscribe(SubscriberConnection conn, string topicName)
        {
            if (string.IsNullOrWhiteSpace(topicName)) return;
            if (_topics.TryGetValue(topicName.Trim(), out var topic))
            {
                lock (topic.Lock) { topic.Subscribers.Remove(conn.SubscriberId); }
                SaveSubscriptions();
                Console.WriteLine($"[Broker] '{conn.SubscriberId}' dezabonat de la '{topicName.Trim()}'.");
            }
        }

        // ------------------------------------------------------------- PERSISTENTA

        private static string TopicFilePath(string topic)
        {
            // numele fisierului: topicul in litere mici, fara caractere invalide
            string safe = string.Concat(topic.ToLowerInvariant()
                .Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' ? c : '_'));
            return Path.Combine(DataDirectory, safe + ".log");
        }

        // La pornire: reincarca istoricul fiecarui topic din fisierele .log si abonarile din subscriptions.json
        private static void LoadPersistedState()
        {
            Directory.CreateDirectory(DataDirectory);

            int loaded = 0;
            foreach (string file in Directory.GetFiles(DataDirectory, "*.log"))
            {
                foreach (string line in File.ReadLines(file))
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    try
                    {
                        var message = JsonSerializer.Deserialize<ChatMessage>(line);
                        if (message == null || string.IsNullOrWhiteSpace(message.Topic)) continue;
                        _topics.GetOrAdd(message.Topic, _ => new TopicState()).History.Add(message);
                        loaded++;
                    }
                    catch (JsonException)
                    {
                        // linie corupta in fisier -> o sarim, restul se incarca normal
                    }
                }
            }

            if (File.Exists(SubscriptionsFile))
            {
                try
                {
                    var map = JsonSerializer.Deserialize<Dictionary<string, List<string>>>(File.ReadAllText(SubscriptionsFile));
                    if (map != null)
                    {
                        foreach (var entry in map)
                        {
                            var topic = _topics.GetOrAdd(entry.Key, _ => new TopicState());
                            foreach (string subscriberId in entry.Value) topic.Subscribers.Add(subscriberId);
                        }
                    }
                }
                catch (JsonException)
                {
                    Console.WriteLine("[Broker] subscriptions.json este corupt, abonarile nu au fost reincarcate.");
                }
            }

            Console.WriteLine($"[Broker] Reincarcat de pe disc: {loaded} mesaje in {_topics.Count} topicuri.");
        }

        // Scrie pe disc, pe rand, mesajele din coada (un singur task -> fara conflicte pe fisier)
        private static async Task PersistWriterAsync()
        {
            await foreach (ChatMessage message in _persistQueue.Reader.ReadAllAsync())
            {
                try
                {
                    await File.AppendAllTextAsync(TopicFilePath(message.Topic),
                        JsonSerializer.Serialize(message) + Environment.NewLine);
                }
                catch (IOException ex)
                {
                    Console.WriteLine($"[Broker] Eroare la salvarea pe disc: {ex.Message}");
                }
            }
        }

        // Salveaza lista abonatilor fiecarui topic (ca sa stim dupa repornire cine trebuie sa primeasca mesajele)
        private static void SaveSubscriptions()
        {
            lock (_subsFileLock)
            {
                var map = new Dictionary<string, List<string>>();
                foreach (var entry in _topics)
                {
                    lock (entry.Value.Lock)
                    {
                        if (entry.Value.Subscribers.Count > 0)
                            map[entry.Key] = entry.Value.Subscribers.ToList();
                    }
                }

                Directory.CreateDirectory(DataDirectory);
                File.WriteAllText(SubscriptionsFile, JsonSerializer.Serialize(map, _indented));
            }
        }

        // ------------------------------------------------------- MESAJE ESUATE (fisier)

        private static void EnsureFailedMessagesFileExists()
        {
            if (!File.Exists(FailedMessagesFile))
                File.WriteAllText(FailedMessagesFile, "[]");
        }

        // Fiecare intrare = un mesaj care nu a ajuns la un anumit destinatar, cu motivul.
        // Mesajul ramane oricum in istoricul topicului, deci subscriber-ul il primeste la reconectare.
        private static void AppendFailed(ChatMessage message, string subscriberId, string reason)
        {
            lock (_failedFileLock)
            {
                List<FailedMessage> failed;
                try
                {
                    failed = JsonSerializer.Deserialize<List<FailedMessage>>(File.ReadAllText(FailedMessagesFile))
                             ?? new List<FailedMessage>();
                }
                catch
                {
                    failed = new List<FailedMessage>();
                }

                failed.Add(new FailedMessage
                {
                    Message = message,
                    TargetSubscriberId = subscriberId,
                    Reason = reason
                });
                File.WriteAllText(FailedMessagesFile, JsonSerializer.Serialize(failed, _indented));
            }
        }
    }
}
