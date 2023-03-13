import time
from solace.messaging.messaging_service import MessagingService
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver
from solace.messaging.resources.topic import Topic

# Create a Solace messaging service instance
service = MessagingService.builder().from_properties(
    host='<host>',
    vpn_name='<vpn-name>',
    username='<username>',
    password='<password>').build()

# Create a message publisher and topic
publisher = DirectMessagePublisher.builder().build(service)
topic = Topic.of('<topic>')

# Publish a message to the topic
publisher.publish(publisher.message_builder().with_text('Hello, world!').build(), topic)

# Create a message receiver and subscribe to the same topic
receiver = DirectMessageReceiver.builder().build(service)
receiver.receive_async(
    topic=topic,
    message_handler=lambda message: print(f"Received message: {message.get_payload_as_text()}"))

# Wait for messages
print('Waiting for messages...')
time.sleep(10)

# Cleanup
receiver.close()
publisher.close()
service.dispose()

