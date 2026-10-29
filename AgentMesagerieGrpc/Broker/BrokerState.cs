using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using MessagingAgent.Contracts;

namespace MessagingAgent.Broker
{
    // Un topic = un canal logic: istoricul complet al mesajelor + ID-urile abonatilor
    // (lista abonatilor ramane si cat timp un abonat e offline).
    public class TopicState
    {
        public readonly object Lock = new();
        public readonly List<ChatMessage> History = new();
        public readonly HashSet<string> Subscribers = new();
    }

    // Un element din coada de iesire a unui subscriber. IsReplay = true pentru mesajele din istoric
    // (retrimise la conectare); cele "live" sunt mesaje noi.
    public readonly record struct OutgoingMessage(ChatMessage Message, bool IsReplay);

    // Conexiunea live a unui subscriber. Toate mesajele pentru el (istoric + live) trec prin coada
    // Outbox, iar un singur task (fluxul gRPC al conexiunii) le scrie pe retea, pe rand -> ordine corecta,
    // fara scrieri simultane pe acelasi stream.
    public class SubscriberConnection
    {
        public string SubscriberId { get; }

        public Channel<OutgoingMessage> Outbox { get; } =
            Channel.CreateUnbounded<OutgoingMessage>(new UnboundedChannelOptions { SingleReader = true });

        // topicurile pentru care istoricul a fost deja pus in coada (de aici incolo primim mesaje live)
        public ConcurrentDictionary<string, bool> ActiveTopics { get; } = new(StringComparer.OrdinalIgnoreCase);

        public SubscriberConnection(string subscriberId) => SubscriberId = subscriberId;
    }

    // O intrare din failed_messages.json
    internal sealed class FailedEntry
    {
        public string MessageId { get; set; } = "";
        public string SenderId { get; set; } = "";
        public string Topic { get; set; } = "";
        public string Content { get; set; } = "";
        public DateTime? SentAt { get; set; }
        public string TargetSubscriberId { get; set; } = "";
        public string Reason { get; set; } = "";
        public DateTime FailedAt { get; set; } = DateTime.UtcNow;
    }

    // Toata starea si logica broker-ului. Este un singleton: serviciile gRPC (care ruleaza concurent,
    // cate un apel pe fir) folosesc aceeasi instanta, de aceea totul e thread-safe.
    public sealed class BrokerState
    {
        private const string DataDirectory = "broker-data";
        private const string FailedMessagesFile = "failed_messages.json";
        private const int MaxIdLength = 50;
        private const int MaxTopicLength = 64;
        private const int MaxContentLength = 4000;

        private static readonly string SubscriptionsFile = Path.Combine(DataDirectory, "subscriptions.json");
        private static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };

        // numele topicului -> starea lui
        private readonly ConcurrentDictionary<string, TopicState> _topics = new(StringComparer.OrdinalIgnoreCase);

        // subscriberId -> conexiunea curenta (exista doar cat timp subscriberul e online)
        private readonly ConcurrentDictionary<string, SubscriberConnection> _connected = new();

        // coada de scriere pe disc: Publish pune mesajul in coada (instant), un task de fundal il scrie
        private readonly Channel<ChatMessage> _persistQueue =
            Channel.CreateUnbounded<ChatMessage>(new UnboundedChannelOptions { SingleReader = true });

        private readonly object _failedFileLock = new();
        private readonly object _subsFileLock = new();

        public BrokerState()
        {
            Directory.CreateDirectory(DataDirectory);
            if (!File.Exists(FailedMessagesFile)) File.WriteAllText(FailedMessagesFile, "[]");

            LoadPersistedState();
            _ = Task.Run(PersistWriterAsync);

            Console.WriteLine($"[Broker] Date persistente: {Path.GetFullPath(DataDirectory)}");
            Console.WriteLine($"[Broker] Mesaje esuate: {Path.GetFullPath(FailedMessagesFile)}");
        }

        // ------------------------------------------------------------------ VALIDARE

        public static string? ValidateId(string? id, string what)
        {
            if (string.IsNullOrWhiteSpace(id)) return $"{what} este gol";
            if (id.Trim().Length > MaxIdLength) return $"{what} este prea lung (max {MaxIdLength} caractere)";
            return null;
        }

        public static string? ValidateTopic(string? topic)
        {
            if (string.IsNullOrWhiteSpace(topic)) return "topicul este gol";
            if (topic.Trim().Length > MaxTopicLength) return $"topicul este prea lung (max {MaxTopicLength} caractere)";
            return null;
        }

        // ------------------------------------------------------------------ PUBLISH

        public PublishReply Publish(PublishRequest request)
        {
            string? error = ValidateId(request.SenderId, "SenderId") ?? ValidateTopic(request.Topic);
            if (error == null && string.IsNullOrWhiteSpace(request.Content)) error = "continutul este gol";
            if (error == null && request.Content.Length > MaxContentLength)
                error = $"continutul este prea lung (max {MaxContentLength} caractere)";

            if (error != null)
            {
                Console.WriteLine($"[Broker] Publish respins: {error}");
                WriteFailed(new FailedEntry
                {
                    SenderId = request.SenderId,
                    Topic = request.Topic,
                    Content = request.Content,
                    TargetSubscriberId = "(niciunul)",
                    Reason = "mesaj invalid: " + error
                });
                return new PublishReply { Accepted = false, Error = error };
            }

            string topicName = request.Topic.Trim();
            var message = new ChatMessage
            {
                Id = Guid.NewGuid().ToString(),
                SenderId = request.SenderId.Trim(),
                Topic = topicName,
                Content = request.Content,
                Timestamp = Timestamp.FromDateTime(DateTime.UtcNow)
            };

            TopicState topic = _topics.GetOrAdd(topicName, _ => new TopicState());
            var failedTargets = new List<(string SubscriberId, string Reason)>();
            int subscribers, delivered = 0;

            // adaugarea in istoric si punerea in cozile abonatilor se fac atomic (sub lock-ul topicului):
            // un subscriber care se aboneaza chiar atunci primeste mesajul o singura data
            // (fie prin istoric, fie live), niciodata de doua ori si niciodata deloc
            lock (topic.Lock)
            {
                topic.History.Add(message);
                _persistQueue.Writer.TryWrite(message);
                subscribers = topic.Subscribers.Count;

                foreach (string subscriberId in topic.Subscribers)
                {
                    if (!_connected.TryGetValue(subscriberId, out SubscriberConnection? conn))
                    {
                        failedTargets.Add((subscriberId, "subscriber offline"));
                        continue;
                    }

                    // conectat, dar istoricul acestui topic nu a fost inca pus in coada:
                    // mesajul e deja in istoric, deci va ajunge odata cu el
                    if (!conn.ActiveTopics.ContainsKey(topicName)) continue;

                    if (conn.Outbox.Writer.TryWrite(new OutgoingMessage(message, false))) delivered++;
                    else failedTargets.Add((subscriberId, "conexiune inchisa"));
                }
            }

            if (subscribers == 0)
                failedTargets.Add(("(niciunul)", "niciun subscriber abonat la topic"));

            foreach (var (subscriberId, reason) in failedTargets)
            {
                Console.WriteLine($"[Broker] [{topicName}] mesaj {message.Id} NU a ajuns la '{subscriberId}': {reason}. Salvat ca esuat.");
                RecordFailed(message, subscriberId, reason);
            }

            Console.WriteLine($"[Broker] [{topicName}] mesaj {message.Id} de la '{message.SenderId}': {subscribers} abonati, {delivered} online.");
            return new PublishReply
            {
                Accepted = true,
                MessageId = message.Id,
                Subscribers = subscribers,
                Delivered = delivered
            };
        }

        // ------------------------------------------------------------------ SUBSCRIBER

        // Apelat la deschiderea fluxului Connect: inregistreaza conexiunea si pune in coada istoricul
        // topicurilor la care acest ID era deja abonat (reabonare automata, abonarile sunt salvate pe disc).
        public SubscriberConnection Connect(string subscriberId)
        {
            var conn = new SubscriberConnection(subscriberId);

            // o conexiune veche cu acelasi ID este inchisa (cea noua o inlocuieste)
            if (_connected.TryGetValue(subscriberId, out SubscriberConnection? old))
                old.Outbox.Writer.TryComplete();
            _connected[subscriberId] = conn;
            Console.WriteLine($"[Broker] Subscriber '{subscriberId}' conectat.");

            foreach (var entry in _topics.OrderBy(t => t.Key, StringComparer.OrdinalIgnoreCase))
            {
                lock (entry.Value.Lock)
                {
                    if (!entry.Value.Subscribers.Contains(subscriberId)) continue;
                    // deja activ = s-a abonat explicit intre timp si a primit deja istoricul
                    if (!conn.ActiveTopics.TryAdd(entry.Key, true)) continue;

                    foreach (ChatMessage m in entry.Value.History)
                        conn.Outbox.Writer.TryWrite(new OutgoingMessage(m, true));

                    Console.WriteLine($"[Broker] '{subscriberId}' reabonat automat la '{entry.Key}' - istoric: {entry.Value.History.Count} mesaje.");
                }
            }

            return conn;
        }

        // Apelat cand fluxul Connect se termina (client deconectat, eroare, inlocuit de o reconectare).
        public void Disconnect(SubscriberConnection conn)
        {
            _connected.TryRemove(new KeyValuePair<string, SubscriberConnection>(conn.SubscriberId, conn));
            conn.Outbox.Writer.TryComplete();

            // mesajele noi ramase in coada nu au ajuns la subscriber (cele din istoric nu sunt esecuri noi)
            while (conn.Outbox.Reader.TryRead(out OutgoingMessage item))
            {
                if (!item.IsReplay)
                    RecordFailed(item.Message, conn.SubscriberId, "conexiune intrerupta inainte de livrare");
            }

            Console.WriteLine($"[Broker] Subscriber '{conn.SubscriberId}' deconectat.");
        }

        public StatusReply Subscribe(string subscriberId, string topicName)
        {
            string? error = ValidateId(subscriberId, "SubscriberId") ?? ValidateTopic(topicName);
            if (error != null) return new StatusReply { Ok = false, Error = error };

            subscriberId = subscriberId.Trim();
            topicName = topicName.Trim();
            TopicState topic = _topics.GetOrAdd(topicName, _ => new TopicState());

            int replayed = 0;
            lock (topic.Lock)
            {
                topic.Subscribers.Add(subscriberId);

                // daca e conectat acum: pune istoricul in coada si marcheaza topicul ca activ
                // (daca nu e conectat, istoricul ii e trimis la urmatoarea conectare)
                if (_connected.TryGetValue(subscriberId, out SubscriberConnection? conn))
                {
                    conn.ActiveTopics[topicName] = true;
                    foreach (ChatMessage m in topic.History)
                    {
                        conn.Outbox.Writer.TryWrite(new OutgoingMessage(m, true));
                        replayed++;
                    }
                }
            }

            SaveSubscriptions();
            Console.WriteLine($"[Broker] '{subscriberId}' abonat la '{topicName}' - istoric trimis: {replayed} mesaje.");
            return new StatusReply { Ok = true };
        }

        public StatusReply Unsubscribe(string subscriberId, string topicName)
        {
            string? error = ValidateId(subscriberId, "SubscriberId") ?? ValidateTopic(topicName);
            if (error != null) return new StatusReply { Ok = false, Error = error };

            subscriberId = subscriberId.Trim();
            topicName = topicName.Trim();

            if (_topics.TryGetValue(topicName, out TopicState? topic))
            {
                lock (topic.Lock)
                {
                    topic.Subscribers.Remove(subscriberId);
                    if (_connected.TryGetValue(subscriberId, out SubscriberConnection? conn))
                        conn.ActiveTopics.TryRemove(topicName, out _);
                }
                SaveSubscriptions();
                Console.WriteLine($"[Broker] '{subscriberId}' dezabonat de la '{topicName}'.");
            }

            return new StatusReply { Ok = true };
        }

        public ListTopicsReply ListTopics(string subscriberId)
        {
            var reply = new ListTopicsReply();
            foreach (var entry in _topics.OrderBy(t => t.Key, StringComparer.OrdinalIgnoreCase))
            {
                lock (entry.Value.Lock)
                {
                    reply.Topics.Add(new TopicInfo
                    {
                        Name = entry.Key,
                        Messages = entry.Value.History.Count,
                        Subscribers = entry.Value.Subscribers.Count,
                        Subscribed = entry.Value.Subscribers.Contains(subscriberId)
                    });
                }
            }
            return reply;
        }

        // ------------------------------------------------------------------ PERSISTENTA

        private static string TopicFilePath(string topic)
        {
            // numele fisierului: topicul in litere mici, fara caractere invalide
            string safe = string.Concat(topic.ToLowerInvariant()
                .Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' ? c : '_'));
            return Path.Combine(DataDirectory, safe + ".log");
        }

        // La pornire: reincarca istoricul din fisierele .log si abonarile din subscriptions.json
        private void LoadPersistedState()
        {
            int loaded = 0;
            foreach (string file in Directory.GetFiles(DataDirectory, "*.log"))
            {
                foreach (string line in File.ReadLines(file))
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    try
                    {
                        ChatMessage message = ChatMessage.Parser.ParseJson(line);
                        if (string.IsNullOrWhiteSpace(message.Topic)) continue;
                        _topics.GetOrAdd(message.Topic, _ => new TopicState()).History.Add(message);
                        loaded++;
                    }
                    catch (Exception)
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
                            TopicState topic = _topics.GetOrAdd(entry.Key, _ => new TopicState());
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

        // Scrie pe disc, pe rand, mesajele din coada (un singur task -> fara conflicte pe fisier).
        // Format: un JSON pe linie (Protobuf JSON), fisier in care se doar adauga.
        private async Task PersistWriterAsync()
        {
            await foreach (ChatMessage message in _persistQueue.Reader.ReadAllAsync())
            {
                try
                {
                    await File.AppendAllTextAsync(TopicFilePath(message.Topic),
                        JsonFormatter.Default.Format(message) + Environment.NewLine);
                }
                catch (IOException ex)
                {
                    Console.WriteLine($"[Broker] Eroare la salvarea pe disc: {ex.Message}");
                }
            }
        }

        // Salveaza lista abonatilor fiecarui topic (ca dupa repornire sa stim cine trebuie sa primeasca mesajele)
        private void SaveSubscriptions()
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

                File.WriteAllText(SubscriptionsFile, JsonSerializer.Serialize(map, Indented));
            }
        }

        // ------------------------------------------------------------------ MESAJE ESUATE

        // Fiecare intrare = un mesaj care nu a ajuns la un anumit destinatar, cu motivul.
        // Mesajul ramane oricum in istoricul topicului, deci subscriberul il primeste la reconectare.
        public void RecordFailed(ChatMessage message, string targetSubscriberId, string reason)
        {
            WriteFailed(new FailedEntry
            {
                MessageId = message.Id,
                SenderId = message.SenderId,
                Topic = message.Topic,
                Content = message.Content,
                SentAt = message.Timestamp?.ToDateTime(),
                TargetSubscriberId = targetSubscriberId,
                Reason = reason
            });
        }

        private void WriteFailed(FailedEntry entry)
        {
            lock (_failedFileLock)
            {
                List<FailedEntry> failed;
                try
                {
                    failed = JsonSerializer.Deserialize<List<FailedEntry>>(File.ReadAllText(FailedMessagesFile))
                             ?? new List<FailedEntry>();
                }
                catch
                {
                    failed = new List<FailedEntry>();
                }

                failed.Add(entry);
                File.WriteAllText(FailedMessagesFile, JsonSerializer.Serialize(failed, Indented));
            }
        }
    }
}
