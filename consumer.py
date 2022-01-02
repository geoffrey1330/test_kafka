from KafkaComponents import Kafka_Consumer
from time import sleep

kfcon = Kafka_Consumer(topic="testing")


def main():
    for message in kfcon.consumer:
        print(message.value)
        sleep(2)

if __name__ == "__main__":
    main()