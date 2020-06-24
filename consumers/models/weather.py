"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""

        # abort if message not from weather topic
        if message.topic() != "cta.weather":
            return

        # otherwise set temp and status
        weather = message.value()
        self.temparature = weather.get("temparature")
        self.status = weather.get("status")

# eof 
