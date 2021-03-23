import uuid
import random
import string


def generate_random_data():
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

print(generate_random_data())
