from kafka import KafkaProducer, KafkaConsumer
from json import loads
from json import dumps


class Kafka_Consumer:
    def __init__(self, topic='GetEvent', bootstrap_servers=['157.230.219.54:9092'], auto_offset_reset="latest", group_id=None):
        """This function is used to create a kafka consumer."""
        self.consumer = KafkaConsumer(
            topic,  # same topic used to publish
            # default kafka server: ip address and port
            bootstrap_servers=bootstrap_servers,
            # enables the consumer to read msgs after restarting (due to loss in connection)
            auto_offset_reset=auto_offset_reset,
            group_id=group_id,  # consumer group enables auto commit
            enable_auto_commit=True,
            #  consumer_timeout_ms=1000,  # interval between two commits has a default value of 1000ms.
            value_deserializer=lambda x: loads(
                x.decode('utf-8'))  # load the json data
        )


class Kafka_Producer:
    def __init__(self, topic='NewEvent', bootstrap_servers=['157.230.219.54:9092']):
        """This function is used to create a kafka producer."""
        self.topic = topic
        self.producer = None
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,  # default kafka server ip address and port
                value_serializer=lambda x: dumps(x).encode(
                    'utf-8')  # covert the data to json and encode it
            )
        except Exception as e:
            print("An error occured while connecting Kafka")
            print(e)
        return

    def publish_message(self, data):
        """This function is used to by the producer to publish videoURLs to the topic."""
        topic = self.topic
        try:
            # send the data i.e (topic, data)
            self.producer.send(topic, value=data)
            self.producer.flush()   # makes all buffered records immediately available to send
        except Exception as e:
            print("An error occured while publishing the message")
            print(e)
