# Agent de mesagerie — Partea 2: gRPC (C#)

Aceeași arhitectură ca în Partea 1 (Publisher / Broker / Subscriber, topicuri, istoric, persistență,
reabonare automată, mesaje eșuate), dar comunicarea se face prin **RPC (gRPC)**, iar datele sunt
reprezentate în **Protocol Buffers** (`.proto`) în loc de JSON peste socketuri.

## Ce s-a schimbat față de Partea 1

| | Partea 1 (sockets) | Partea 2 (gRPC) |
| :--- | :--- | :--- |
| Comunicare | `TcpListener` / `TcpClient`, citit/scris linii manual | apeluri gRPC (metode generate din `.proto`) |
| Reprezentarea datelor | JSON, un mesaj pe linie | Protobuf (binar), definit în `messaging.proto` |
| Transport | TCP | HTTP/2 (peste TCP) |
| Cadrare mesaje | manual (newline) | gestionată de gRPC |
| Publish | scrii un JSON pe socket | apel `Publish` (unary), primești confirmare |
| Primire mesaje | citești linii de pe socket | flux `Connect` (server streaming) |
| Abonare / listare topicuri | mesaje JSON speciale | apeluri `Subscribe`, `Unsubscribe`, `ListTopics` |
| Validare | în cod | în cod + tipuri stricte din schemă |
| Porturi | 5000 publisheri, 5001 subscriberi | la fel: 5000 și 5001 |

## Contractul (`Protos/messaging.proto`, același fișier în cele 3 proiecte)

```
service Publisher {                       // portul 5000
  rpc Publish (PublishRequest) returns (PublishReply);                // unary
}
service Subscriber {                      // portul 5001
  rpc Connect (ConnectRequest) returns (stream ChatMessage);          // server streaming
  rpc Subscribe (TopicRequest) returns (StatusReply);                 // unary
  rpc Unsubscribe (TopicRequest) returns (StatusReply);               // unary
  rpc ListTopics (ListTopicsRequest) returns (ListTopicsReply);       // unary
}
```

- **Unary** = cerere → răspuns (ca un apel obișnuit de metodă).
- **Server streaming** = o cerere → un flux de răspunsuri care rămâne deschis. Îl folosim pentru
  `Connect`: broker-ul trimite întâi istoricul, apoi mesajele noi, în timp real.
- Din `.proto` se generează automat clasele de mesaje și „stub”-urile: `Publisher.PublisherClient`
  (în Sender), `Subscriber.SubscriberClient` (în Receiver) și clasele de bază `PublisherBase` /
  `SubscriberBase` pe care broker-ul le implementează.

## Cum lucrează broker-ul

- **Două porturi**: Kestrel ascultă pe 5000 și 5001 (HTTP/2). `RequireHost` leagă fiecare serviciu de
  portul lui: `Publisher` doar pe 5000, `Subscriber` doar pe 5001.
- **Concurență**: fiecare apel gRPC rulează pe propriul fir (ASP.NET Core), deci cererile sunt
  tratate concurent. Starea comună (`BrokerState`) este un singleton thread-safe
  (`ConcurrentDictionary`, lock pe topic).
- **Cozi de livrare**: fiecare subscriber conectat are o coadă de ieșire (`Channel`). `Publish` pune
  mesajul în cozile abonaților online; fluxul `Connect` al fiecărui subscriber le scrie pe rețea, pe
  rând. Astfel istoricul și mesajele noi nu se amestecă, iar ordinea e corectă.
- **Publish** (`BrokerState.Publish`): validează (SenderId, topic, conținut: nenule și cu lungime maximă),
  adaugă mesajul în istoric, îl pune în coada de salvare pe disc și în cozile abonaților online — totul
  sub lock-ul topicului, ca un abonat care se abonează simultan să primească mesajul o singură dată.
- **Reabonare automată**: la `Connect`, broker-ul reabonează ID-ul la topicurile lui salvate și pune în
  coadă istoricul lor. Fără `sub` manual.
- **Persistență**: istoricul în `broker-data/<topic>.log` (un JSON Protobuf pe linie), abonările în
  `broker-data/subscriptions.json`; scrierea istoricului se face asincron, printr-o coadă și un task de
  fundal. La pornire broker-ul reîncarcă tot.
- **Mesaje eșuate** → `failed_messages.json`, cu destinatarul, motivul și ora: abonat offline, conexiune
  întreruptă înainte de livrare, niciun abonat pe topic, mesaj invalid (respins la validare).
  Mesajele rămân în istoric, deci abonatul le primește la reconectare.

## Cum rulați (.NET 8 SDK, internet la primul build pentru pachetele NuGet)

```bash
# 1) broker (o singură dată)
cd Broker && dotnet run

# 2) unul sau mai mulți subscriberi (terminale separate)
cd Receiver && dotnet run
#   Subscriber ID: S1   -> apare lista topicurilor; abonare: sub sport

# 3) unul sau mai mulți publisheri
cd Sender && dotnet run
#   Publisher ID: P1    Topic: sport    Mesaj: salut
```

Prima rulare face `dotnet restore` automat și descarcă `Grpc.AspNetCore`, `Grpc.Net.Client`,
`Google.Protobuf`, `Grpc.Tools`. Pentru instanțe multiple din Visual Studio: faceți build o dată, apoi
porniți direct `.exe`-urile din `bin\Debug\net8.0\`.

## Pe laptopuri diferite

1. În `Sender/Program.cs` și `Receiver/Program.cs` schimbați `BrokerAddress` din `127.0.0.1` în IP-ul
   laptopului cu broker-ul (`ipconfig`), de exemplu `http://192.168.1.10:5000` pentru Sender și
   `http://192.168.1.10:5001` pentru Receiver.
2. Aceeași rețea pentru toate laptopurile.
3. Pe laptopul cu broker, firewall-ul trebuie să permită porturile (PowerShell ca administrator):
   `netsh advfirewall firewall add rule name="Broker" dir=in action=allow protocol=TCP localport=5000,5001`

## Scenarii de test

1. **Mai mulți subscriberi pe un topic**: S1 și S2 fac `sub sport`; P1 publică → ambii primesc.
2. **Mai multe topicuri**: S1 face `sub sport` și `sub stiri`; mesajele vin cu eticheta topicului.
3. **Listare topicuri**: `topics` în Receiver → nume, nr. mesaje, nr. abonați, `[abonat]`.
4. **Reconectare**: închideți S1 (`exit`), P1 publică 2-3 mesaje, reporniți S1 cu același ID → vede lista,
   este reabonat automat și primește tot istoricul, fără `sub`.
5. **Persistență**: opriți broker-ul (Ctrl+C), reporniți-l, conectați S1 → istoricul e încă acolo.
6. **Mesaje eșuate**: mesajele publicate cât S1 era offline apar în `failed_messages.json`.
7. **Validare**: publicați cu mesaj gol → `Respins de broker: continutul este gol`.

## Fișiere

```
AgentMesagerieGrpc/
├── Broker/     Program.cs (porturi, DI), GrpcServices.cs (servicii gRPC), BrokerState.cs (logică),
│               Protos/messaging.proto, Broker.csproj
├── Sender/     Program.cs (publisher), Protos/messaging.proto, Sender.csproj
├── Receiver/   Program.cs (subscriber), Protos/messaging.proto, Receiver.csproj
└── README.md
```

Dacă modificați `messaging.proto`, copiați același fișier în toate cele trei proiecte.
