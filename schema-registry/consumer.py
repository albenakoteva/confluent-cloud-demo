from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka import Consumer


class User(object):
    def __init__(self, name, age):
        self.name = name
        self.age = age


def dict_to_user(obj, ctx):
    if obj is None:
        return None

    return User(obj["name"], obj["age"])


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


def get_consumer_config(config):
    consumer_conf = {
        "bootstrap.servers": f"{config["bootstrap.servers"]}",
        "security.protocol": f"{config["security.protocol"]}",
        "sasl.mechanisms": f"{config["sasl.mechanisms"]}",
        "sasl.username": f"{config["sasl.username"]}",
        "sasl.password": f"{config["sasl.password"]}",
        "group.id": "python-group-1",
        "auto.offset.reset": "latest",
    }
    return consumer_conf


def main():
    topic_name = "topic_100"
    schema = get_schema()
    config = read_config()
    json_deserializer = JSONDeserializer(schema, from_dict=dict_to_user)
    consumer = Consumer(get_consumer_config(config))
    consumer.subscribe([topic_name])
    while True:
        try:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            user = json_deserializer(
                msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
            )
        except Exception as e:
            print("Exception in consumer is ", e)
            break
        else:
            print("The user is ", user.__dict__)

    consumer.close()


if __name__ == "__main__":
    main()