# ADD kafka.so path
import sys
sys.path.append(".")
sys.path.append("..")
import kafka

import unittest

import random
import string

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

class TestKafkaLib(unittest.TestCase):

    def test_lib_contains_funcs(self):
        self.assertTrue("consume" in dir(kafka))
        self.assertTrue("produce" in dir(kafka))

    def test_produce_consume(self):
        message = get_random_string(12)
        self.assertEqual(None, kafka.produce("LIBKAFKA_PYTHON_TEST_TOPIC", message))
        self.assertEqual(message, kafka.consume("LIBKAFKA_PYTHON_TEST_TOPIC"))

if __name__ == '__main__':
    unittest.main()
