using Grpc.Core;
using MessagingAgent.Contracts;

namespace MessagingAgent.Broker
{
    // Serviciul pentru PUBLISHERI (expus pe portul 5000).
    // Fiecare apel gRPC ruleaza pe propriul fir -> cererile sunt tratate concurent.
    public class PublisherGrpcService : Publisher.PublisherBase
    {
        private readonly BrokerState _state;

        public PublisherGrpcService(BrokerState state) => _state = state;

        public override Task<PublishReply> Publish(PublishRequest request, ServerCallContext context)
        {
            return Task.FromResult(_state.Publish(request));
        }
    }

    // Serviciul pentru SUBSCRIBERI (expus pe portul 5001).
    public class SubscriberGrpcService : Subscriber.SubscriberBase
    {
        private readonly BrokerState _state;

        public SubscriberGrpcService(BrokerState state) => _state = state;

        // Server streaming: fluxul ramane deschis cat timp subscriberul e conectat.
        // Broker-ul pune mesajele in coada conexiunii (Outbox), iar aici le scriem pe retea, pe rand.
        public override async Task Connect(ConnectRequest request, IServerStreamWriter<ChatMessage> responseStream, ServerCallContext context)
        {
            string? error = BrokerState.ValidateId(request.SubscriberId, "SubscriberId");
            if (error != null)
                throw new RpcException(new Status(StatusCode.InvalidArgument, error));

            SubscriberConnection conn = _state.Connect(request.SubscriberId.Trim());
            OutgoingMessage? inFlight = null;

            try
            {
                await foreach (OutgoingMessage item in conn.Outbox.Reader.ReadAllAsync(context.CancellationToken))
                {
                    inFlight = item;
                    await responseStream.WriteAsync(item.Message);
                    inFlight = null;
                }
            }
            catch (OperationCanceledException)
            {
                // clientul s-a deconectat (sau broker-ul se opreste)
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Broker] Eroare pe fluxul subscriberului '{conn.SubscriberId}': {ex.Message}");
            }
            finally
            {
                // mesajul a fost scos din coada, dar scrierea pe retea nu s-a terminat -> nu a ajuns
                if (inFlight is { IsReplay: false } failed)
                    _state.RecordFailed(failed.Message, conn.SubscriberId, "eroare la trimitere");

                _state.Disconnect(conn);
            }
        }

        public override Task<StatusReply> Subscribe(TopicRequest request, ServerCallContext context)
        {
            return Task.FromResult(_state.Subscribe(request.SubscriberId, request.Topic));
        }

        public override Task<StatusReply> Unsubscribe(TopicRequest request, ServerCallContext context)
        {
            return Task.FromResult(_state.Unsubscribe(request.SubscriberId, request.Topic));
        }

        public override Task<ListTopicsReply> ListTopics(ListTopicsRequest request, ServerCallContext context)
        {
            return Task.FromResult(_state.ListTopics(request.SubscriberId?.Trim() ?? ""));
        }
    }
}
