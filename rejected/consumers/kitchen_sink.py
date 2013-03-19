"""
The KitchenSinkConsumer includes all the consumer types in one, easy to
use but heavy consumer class to extend.

"""
from rejected.consumers import attribute
from rejected.consumers import decoding
from rejected.consumers import deserialization
from rejected.consumers import validating


class KitchenSinkConsumer(attribute.AttributeConsumer,
                          decoding.DecodingConsumer,
                          deserialization.DeserializingConsumer,
                          validating.ValidatingExpirationConsumer,
                          validating.ValidatingTypeConsumer):
    pass
