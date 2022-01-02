from KafkaComponents import Kafka_Producer


kfpro = Kafka_Producer(topic="diet")


def Food(input=""):

    data = input

    kfpro.publish_message(data)


if __name__ == "__main__":
    Food(input="Rice")
