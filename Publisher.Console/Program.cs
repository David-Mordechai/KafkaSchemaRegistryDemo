using Avro;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

const string bootstrapServers = "localhost:9092";
const string schemaRegistryUrl = "localhost:8081";
const string topicName = "users";

var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrapServers
};

var schemaRegistryConfig = new SchemaRegistryConfig
{
    // Note: you can specify more than one schema registry url using the
    // schema.registry.url property for redundancy (comma separated list). 
    // The property name is not plural to follow the convention set by
    // the Java implementation.
    Url = schemaRegistryUrl,
};

var avroSerializerConfig = new AvroSerializerConfig
{
    // optional Avro serializer properties:
    BufferBytes = 100
};


using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
using var producer = new ProducerBuilder<string, User>(producerConfig)
        .SetValueSerializer(new AvroSerializer<User>(schemaRegistry, avroSerializerConfig))
        .Build();
Console.WriteLine($"{producer.Name} producing on {topicName}. Enter user names, q to exit.");

var i = 0;
string? text;
while ((text = Console.ReadLine()) != "q")
{
    var user = new User { FullName = text, FavoriteColor = "Green", FavoriteNumber = i++ };
    var result = await producer
        .ProduceAsync(topicName, new Message<string, User> { Key = text ?? string.Empty, Value = user })
        .ContinueWith(task => task.IsFaulted
            ? $"error producing message: {task.Exception?.Message}"
            : $"produced to: {task.Result.TopicPartitionOffset}");

    Console.WriteLine(result);
}