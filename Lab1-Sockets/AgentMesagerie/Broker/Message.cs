using System;
using System.Text.Json.Serialization;

namespace MessagingAgent.Broker
{
    // Tipurile de mesaje din protocolul de lucru
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public enum MessageType
    {
        Subscribe,     // subscriber -> broker: "aboneaza-ma la acest topic"
        Unsubscribe,   // subscriber -> broker: "anuleaza abonarea la acest topic"
        Publish,       // publisher  -> broker: "iata un mesaj nou pentru acest topic"
        Delivery,      // broker -> subscriber: "iata un mesaj de pe un topic la care esti abonat"
        ListTopics,    // subscriber -> broker: "spune-mi ce topicuri exista"
        TopicList,     // broker -> subscriber: lista topicurilor (in campul Content, cate unul pe linie)
    }

    // Format JSON comun pentru toate mesajele transmise pe retea
    public class ChatMessage
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public MessageType Type { get; set; }
        public string SenderId { get; set; } = string.Empty;      // cine a publicat mesajul
        public string SubscriberId { get; set; } = string.Empty;  // folosit doar la Subscribe/Unsubscribe
        public string Topic { get; set; } = string.Empty;         // canalul logic (subiectul mesajului)
        public string Content { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    }
}
