from confluent_kafka import Producer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

class User(object):
    def __init__(self, name, age):
        self.name = name
        self.age = age


def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config


def get_schema():
  # reads the schema deffinition from geo_location.schema.json
  # and returns a schema deffinition as a text
  f = open("user.schema.json")

  return f.read()


def get_schema_config(config):
  # collect and return a schema registry related configuration
  schema_registry_config = {
    "url": f"{config["schema_registry.endpoint"]}",
    "basic.auth.user.info": f"{config["schema_registry.key"]}:{config["schema_registry.secret"]}"
  }

  return schema_registry_config


def user_to_dict(user, ctx):
    return dict(name=user.name, age=user.age)


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record", err)
    else:
        print("your message was successfully produced", msg)


def get_producer(config):
    producer = Producer(
        {
            "bootstrap.servers": f"{config["bootstrap.servers"]}",
            "security.protocol": f"{config["security.protocol"]}",
            "sasl.mechanisms": f"{config["sasl.mechanisms"]}",
            "sasl.username": f"{config["sasl.username"]}",
            "sasl.password": f"{config["sasl.password"]}",
        }
    )
    return producer


def main():
    topic_name = "topic_100"
    schema = get_schema()
    config = read_config()
    schema_client = SchemaRegistryClient(get_schema_config(config))
    key_serializer = StringSerializer()
    value_serializer = JSONSerializer(schema, schema_client, user_to_dict)
    producer = get_producer(config)

    while True:
        try:
            name = input("Provide name: ")
            age = int(input("Provide age: "))
            user = User(name, age)
            producer.produce(
                topic=topic_name,
                key=key_serializer(user.name),
                value=value_serializer(
                    user, SerializationContext(topic_name, MessageField.VALUE)
                ),
                on_delivery=delivery_report,
            )

        except Exception as e:
            print("Exception is ", e)
            break
        else:
            producer.flush()


if __name__ == "__main__":
    main()