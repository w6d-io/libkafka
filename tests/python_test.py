# ADD kafka.so path
import sys
sys.path.append(".")
sys.path.append("..")


import kafka
print(dir(kafka))
kafka.produce("KAFKA_TOPIC", "hello world")
kafka.consume("KAFKA_TOPIC")
