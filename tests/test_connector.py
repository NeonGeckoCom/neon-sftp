import json
import os
import unittest

from neon_sftp.connector import NeonSFTPConnector


class TestSFTPConnector(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        with open(os.environ.get('SFTP_CREDS_PATH', 'test_config.json')) as f:
            sftp_creds = json.load(f)
            cls.connector = NeonSFTPConnector(**sftp_creds)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.connector.transport.close()
        cls.connector.connection.close()

    def test_get_file(self):
        save_to = 'output_graph.tflite'
        self.connector.get_file(get_from='/de/output_graph.tflite', save_to=save_to)
        self.assertTrue(os.path.exists(save_to))
        os.remove(save_to)
        self.assertTrue(not os.path.exists(save_to))


if __name__ == '__main__':
    unittest.main()
