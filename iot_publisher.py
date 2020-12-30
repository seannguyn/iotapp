from google.cloud import pubsub_v1
import os
import random
import datetime
import time

try:
    project_id = os.getenv("PROJECT_ID")
    topic_id = os.getenv("TOPIC_ID")
except KeyError:
    raise Exception("Please ensure that these environment variables are set: "
                    "PROJECT_ID, "
                    "TOPIC_ID, "
                    "GOOGLE_APPLICATION_CREDENTIALS"
                    )

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

while True:
    try:
        # get data from sensor
        temperature = random.randint(19, 30)
        humidity = random.randint(30, 40)
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Data must be a bytestring
        payload = f'{{"data": "iot_payload_data", "timestamp": "{timestamp}", temperature: {temperature}, humidity: {humidity}}}'
        data = payload.encode("utf-8")

        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data)

        print(f"published: {payload}, "
              f"result={future.result()}")

        time.sleep(10)

    except KeyboardInterrupt:
        raise Exception("Terminate program")

