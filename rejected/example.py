"""Example Rejected Consumer"""
import logging

from rejected import consumer
from rejected.mixins import attribute
from rejected.mixins import content
from rejected.mixins import validation

LOGGER = logging.getLogger(__name__)


class Consumer(attribute.AttributeConsumer):
    """This example simply logs the received message content, using the
    rejected.mixins.attribute.AttributeConsumer to get access to the message
    properties, attributes and body as attributes of the consumer itself.

    """
    def process(self):
        """Process the inbound message"""
        LOGGER.info('Received message %s, a %s message: %r - %r',
                    self.message_id, self.type, self.body, self.message)


class AdditionRPCConsumer(consumer.RPCConsumer,
                          content.DeserializingConsumer,
                          content.JSONPublishingConsumer,
                          validation.TypeValidation):
    """This RPC consumer example extends the RPCConsumer class adding in the
    following mixins:

     - DeserializationConsumer: Automatically deserialize the JSON message body
     - JSONPublishingConsumer: Automatically serialize the response body as JSON
     - TypeConsumer: Validate the message type matches "addition_request"

    """
    MESSAGE_TYPES = ['addition_request']

    def process(self):
        """Reply to an inbound message, summing the values of a JSON
        serialized list passed in to the message body.

        """
        LOGGER.info('Processing request %s: %s(%r)',
                    self.message.properties.message_id,
                    self.message.properties.type,
                    self.message.body)
        self.reply({'sum': sum(self.message.body)})
