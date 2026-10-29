# Agent de mesagerie — Publisher / Broker / Subscriber (C#, Sockets)

Partea 1 a laboratorului (Sockets), cu modelul **Publisher / Subscriber** bazat pe **topicuri**.
Format mesaje: JSON. Transport: TCP.

## Roluri

- **Sender = Publisher** – publică mesaje pe un topic (ex. `sport`). Nu știe cine sunt abonații.
- **Broker** – primește mesajele și le rutează după topic către toți abonații topicului.
- **Receiver = Subscriber** – se abonează la **unul sau mai multe** topicuri și primește mesajele lor.
  Se pot adăuga/scoate topicuri din mers (`sub <topic>`, `unsub <topic>`) și se pot vedea toate
  topicurile existente cu `topics` (nume, nr. mesaje, nr. abonați, `[abonat]` dacă ești deja abonat).

Mai mulți publisheri și mai mulți subscriberi pot rula simultan; mai mulți subscriberi pot fi
abonați la același topic (fiecare primește mesajul).

## Porturi

Broker-ul ascultă pe două porturi separate (conexiuni de rețea independente):

| Port | Cine se conectează |
| :--- | :--- |
| `5000` | Publisheri (Sender) |
| `5001` | Subscriberi (Receiver) |

Pe partea clientului, sistemul de operare alocă automat un port local diferit fiecărei conexiuni,
deci nu există coliziuni între mulți clienți.

## Protocolul de lucru (JSON, un mesaj pe linie)

| `Type` | Direcție | Rol |
| :--- | :--- | :--- |
| `Subscribe` | subscriber → broker | abonare la un topic (`SubscriberId`, `Topic`) |
| `Unsubscribe` | subscriber → broker | dezabonare de la un topic |
| `Publish` | publisher → broker | mesaj nou (`SenderId`, `Topic`, `Content`) |
| `Delivery` | broker → subscriber | mesaj livrat de pe un topic |
| `ListTopics` | subscriber → broker | cerere: lista topicurilor existente |
| `TopicList` | broker → subscriber | răspuns: lista topicurilor (în `Content`, câte unul pe linie) |

- **Cadrare**: câte un JSON pe linie (`WriteLine`/`ReadLine`); JSON-ul e pe o singură linie, deci
  newline-ul e separator sigur.
- **De ce TCP**: livrare garantată și în ordine; broker-ul detectează conexiunile căzute.

## Cum lucrează broker-ul

1. Pentru fiecare topic ține **istoricul complet** al mesajelor și **lista abonaților** (lista
   rămâne și când un abonat e offline).
2. La `Publish`: validează mesajul (topic și conținut nenule), îl adaugă în istoric și îl trimite
   concurent tuturor abonaților conectați (`Task.WhenAll`).
3. La `Subscribe`: adaugă abonatul și îi retransmite **tot istoricul topicului de la început**, apoi
   primește mesajele noi live.
   La **conectare** (prima cerere a unui subscriber, `ListTopics`): brokerul îi trimite **lista
   topicurilor existente**, iar dacă acel `SubscriberId` a mai fost abonat (abonările sunt salvate pe
   disc), îl **reabonează automat** la topicurile lui și îi retransmite istoricul lor — fără să se
   aboneze manual din nou.
   Replay-ul și mesajele noi sunt serializate (semafor pe conexiune + lock pe topic), deci nu apar
   duplicate și ordinea rămâne corectă.
4. **Mesaje eșuate** → `failed_messages.json`: orice mesaj care nu a ajuns la un destinatar
   (abonat offline, eroare de rețea la livrare, niciun abonat pe topic, mesaj invalid), cu
   `TargetSubscriberId`, `Reason` și `FailedAt`. Mesajul rămâne oricum în istoric, deci abonatul îl
   primește la reconectare.
5. **Concurență**: un `Task` per conexiune; colecții thread-safe (`ConcurrentDictionary`) și lock-uri
   scurte pentru starea topicurilor. JSON corupt este ignorat, broker-ul nu cade.
6. **Persistență (metoda persistentă)**: istoricul fiecărui topic se salvează pe disc în
   `broker-data/<topic>.log` (un JSON pe linie, doar se adaugă) și abonările în
   `broker-data/subscriptions.json`. La pornire brokerul le reîncarcă, deci istoricul și lista
   abonaților supraviețuiesc repornirii. Scrierea se face **asincron**: `Publish` pune mesajul într-o
   coadă (`Channel`), iar un singur task de fundal îl scrie pe disc, deci publisherii nu așteaptă
   după fișiere. În memorie, istoricul rămâne în colecții thread-safe pentru acces rapid.

## Cum rulați (.NET 8 SDK)

Mai întâi `dotnet restore` în fiecare folder, apoi:

```bash
# 1) broker (o singură dată)
cd Broker && dotnet run

# 2) unul sau mai mulți subscriberi (terminale separate)
cd Receiver && dotnet run
#   Subscriber ID: S1  -> apare lista topicurilor; abonare cu:  sub sport   /   sub stiri

# 3) unul sau mai mulți publisheri
cd Sender && dotnet run
#   Publisher ID: P1       Topic: sport    Mesaj: salut
```

Pentru instanțe multiple din Visual Studio: faceți build o dată, apoi porniți direct
`bin\Debug\net8.0\Receiver.exe` / `Sender.exe` de câte ori aveți nevoie (evită blocarea fișierelor).

## Scenarii de test

1. **Mai mulți subscriberi pe un topic**: S1 și S2 abonați la `sport`; P1 publică → ambii primesc.
2. **Mai multe topicuri**: S1 abonat la `sport,stiri`; P1 publică pe ambele → S1 le primește pe amândouă,
   fiecare cu eticheta topicului. În S1 scrieți `sub meteo` pentru un topic nou.
3. **Reconectare**: opriți S1 (Ctrl+C), P1 publică 2-3 mesaje, reporniți S1 cu **același ID** →
   vede lista topicurilor (cele la care era abonat au `[abonat]`) și primește automat toată
   conversația de la început, fără `sub`.
4. **Persistență**: opriți Broker-ul (Ctrl+C), reporniți-l, apoi conectați un Subscriber la același
   topic → primește din nou istoricul (reîncărcat din `broker-data`).
5. **Listare topicuri**: în Receiver scrieți `topics` → apare lista topicurilor disponibile.
6. **Mesaje eșuate**: cele publicate cât S1 era offline apar în `failed_messages.json`.

## Fișiere

```
AgentMesagerie/
├── Broker/     (Program.cs, Message.cs, Broker.csproj)
├── Sender/     (Program.cs, Message.cs, Sender.csproj)     <- Publisher
├── Receiver/   (Program.cs, Message.cs, Receiver.csproj)   <- Subscriber
└── README.md
```
