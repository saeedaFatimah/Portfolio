import json  # Make sure this line is present at the top
from kafka import KafkaProducer
from time import sleep
from PollResponseAPI import PollResponseAPI

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initialize Poll Response API
poll_api = PollResponseAPI()

try:
    while True:
        # Generate a sample poll response
        poll_response = json.loads(poll_api.poll_response_api())
        
        # Send response to Kafka topic 'live_poll_responses'
        producer.send('live_poll_responses', poll_response)
        
        # Print confirmation and sleep
        print("Sent response:", poll_response)
        sleep(2)  # Adjust frequency as needed

except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.close()
