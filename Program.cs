using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

Console.WriteLine("FirstMRMQ - RabbitMQ Example");
Console.WriteLine("============================");

// Connection settings
var factory = new ConnectionFactory { HostName = "localhost" };

try
{
    await using var connection = await factory.CreateConnectionAsync();
    await using var channel = await connection.CreateChannelAsync();

    // Declare a queue
    string queueName = "hello";
    await channel.QueueDeclareAsync(queue: queueName,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

    // Send a message
    string message = "Hello from FirstMRMQ!";
    var body = Encoding.UTF8.GetBytes(message);

    await channel.BasicPublishAsync(exchange: string.Empty,
                                     routingKey: queueName,
                                     mandatory: false,
                                     body: body);

    Console.WriteLine($"[x] Sent: {message}");

    // Receive messages
    Console.WriteLine("[*] Waiting for messages...");

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += (model, ea) =>
    {
        var receivedBody = ea.Body.ToArray();
        var receivedMessage = Encoding.UTF8.GetString(receivedBody);
        Console.WriteLine($"[x] Received: {receivedMessage}");
        return Task.CompletedTask;
    };

    await channel.BasicConsumeAsync(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);

    Console.WriteLine("Press [enter] to exit.");
    Console.ReadLine();
}
catch (Exception ex)
{
    Console.WriteLine($"Error connecting to RabbitMQ: {ex.Message}");
    Console.WriteLine("Make sure RabbitMQ is running on localhost.");
}
