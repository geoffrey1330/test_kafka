from KafkaComponents import Kafka_Producer
from time import sleep

kfpro = Kafka_Producer(topic="testing")


def iter():

    for j in range(100):
        print("Iteration", j)
        data = {'counter': j}

        kfpro.publish_message(data)
        sleep(0.5)


if __name__ == "__main__":
    iter()
