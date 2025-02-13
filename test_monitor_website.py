import unittest
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
import monitor_website

class TestMonitorWebsite(unittest.TestCase):

    def test_get_title(self):
        html = "<html><h2>Test Title</h2></html>"
        soup = BeautifulSoup(html, "html.parser")
        self.assertEqual(monitor_website.get_title(soup), "Test Title")

    def test_get_article_content(self):
        html = "<html><article><p>Content</p><p><strong>Not this</strong></p></article></html>"
        soup = BeautifulSoup(html, "html.parser")
        self.assertEqual(monitor_website.get_article_content(soup), "Content")

    def test_get_date(self):
        html = "<html><time>13 de febrero de 2025</time></html>"
        soup = BeautifulSoup(html, "html.parser")
        self.assertEqual(monitor_website.get_date(soup), "2025-02-13")

    @patch('monitor_website.requests.get')
    def test_get_discursos_urls(self, mock_get):
        mock_response = MagicMock()
        mock_response.text = "<html><a href='milei-speech'>Link</a></html>"
        mock_get.return_value = mock_response

        urls = monitor_website.get_discursos_urls("http://example.com", "milei")
        self.assertIn("http://example.commilei-speech", urls)

if __name__ == '__main__':
    unittest.main()
