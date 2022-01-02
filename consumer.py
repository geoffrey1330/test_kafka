from KafkaComponents import Kafka_Consumer

kfcon = Kafka_Consumer(topic="diet")


def main():
    for message in kfcon.consumer:
        print(message.value)
