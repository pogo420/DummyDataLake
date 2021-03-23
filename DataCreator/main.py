import uuid
import random
import string
from google.cloud import pubsub_v1
import json
import os


def generate_random_data() -> dict:
    """Function to generate random data"""
    data = {
        "userId": uuid.uuid4().__str__(),
        "sensorValue": random.random(),
        "sensorId": "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
                    + "-"
                    + "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
                    + "-"
                    + "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
    }
    return data


def pushing_message(project_id: str, topic_id: str, message: str):
    """Function to push message into pubsub topic"""
    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)

    # Data must be a bytestring
    message = message.encode("utf-8")
    # When you publish a message, the client returns a future.
    publisher.publish(topic_path, message)
    print(f"Published messages to {topic_path}.")


def main():
    message = json.dumps(generate_random_data())
    project_id = os.environ["GOOGLE_PROJECT"]
    input_topic = os.environ["INPUT_TOPIC"]
    pushing_message(project_id, input_topic, message)
