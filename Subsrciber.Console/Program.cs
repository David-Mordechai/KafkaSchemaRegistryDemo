using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using ProtoDtos;

const string bootstrapServers = "localhost:9092";
const string schemaRegistryUrl = "localhost:8081";
const string topicName = "users";

var schemaRegistryConfig = new SchemaRegistryConfig
{
    // Note: you can specify more than one schema registry url using the
    // schema.registry.url property for redundancy (comma separated list). 
    // The property name is not plural to follow the convention set by
    // the Java implementation.
    Url = schemaRegistryUrl,
};

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = "protoBuf-example-consumer-group"
};

var cts = new CancellationTokenSource();
var consumeTask = Task.Run(() =>
{
    using var consumer =
        new ConsumerBuilder<string, User>(consumerConfig)
            .SetValueDeserializer(new ProtobufDeserializer<User>().AsSyncOverAsync())
            .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
            .Build();
    consumer.Subscribe(topicName);

    try
    {
        while (true)
        {
            try
            {
                var consumeResult = consumer.Consume(cts.Token);
                Console.WriteLine(
                    $"user name: {consumeResult.Message.Key}, " +
                    $"favorite color: {consumeResult.Message.Value.FavoriteColor}, " +
                    $"favorite number {consumeResult.Message.Value.FavoriteNumber}");
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Consume error: {e.Error.Reason}");
            }
        }
    }
    catch (OperationCanceledException)
    {
        consumer.Close();
    }
});

Console.ReadKey();
cts.Cancel();