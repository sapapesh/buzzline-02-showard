"""
kafka_consumer_sahoward.py

Consume messages from a Kafka topic and process them.
"""

#####################################
# Import Modules
#####################################

import os
import sys
from dotenv import load_dotenv
from kafka import KafkaConsumer  # using kafka-python client
from utils.utils_logger import logger


#####################################
# Keyword Detection
#####################################

FOOD_KEYWORDS = ["gyros", "souvlaki"]


def contains_food(sentence: str) -> bool:
    """Return True if the sentence mentions food keywords."""
    sentence_lower = sentence.lower()
    return any(keyword in sentence_lower for keyword in FOOD_KEYWORDS)


#####################################
# Getter Function for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "buzz_topic")
    logger.info(f"Kafka topic (consumer): {topic}")
    return topic


#####################################
# Main Function
#####################################

def main():
    logger.info("START consumer.")
    logger.info("Loading environment variables from .env file...")
    load_dotenv()

    topic = get_kafka_topic()

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
            group_id="food-detector-group",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: m.decode("utf-8"),
        )
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        sys.exit(1)

    logger.info(f"Listening for messages on topic '{topic}'...")

    try:
        for msg in consumer:
            sentence = msg.value
            if contains_food(sentence):
                logger.info(f"🍴 Food mention detected: {sentence}")
                print(f"🍴 Food mention detected: {sentence}")
            else:
                logger.info(f"Received: {sentence}")
                print(f"Received: {sentence}")
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")
        logger.info("END consumer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()