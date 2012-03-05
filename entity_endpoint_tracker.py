from redis_helpers import *


# We are going to track entities, their endpoings
# and the events their endpoints are subscribed to

NS = 'entity_tracking'

# TODO: rename
class M(object):
    @classmethod
    def get(cls, key):
        i = cls()
        i.key = key
        return i

    @classmethod
    def get_all(cls):
        key_pattern = '%s:%s:*'
        keys = rc.keys(key_pattern)
        return [cls.get(k) for k in keys]

class Entity(M):

    # person's "name"
    handle = ''

    # unique key for this entity
    key = ''

    # list of endpoints
    endpoints = []

class Endpoint(M):

    # entity for endpoint
    entity = None

    # unique name for endpoint
    # (unique for entity)
    name = None

    # unique key (globally) for endpoint
    key = None

    # the url this endpoint is at
    url = None

    # list of events endpoint is subscribed
    # to
    enabled_events = []

class RedisEntity(Entity):
    """
    Entity which stores it's data in redis
    """

    object_name = 'Entity'
    NS = NS

    def __init__(self):
        self.key = None

    # setup our attributes which map to our hash's values
    # handle is the hash key we want to get the value from
    # will check hash @ key <NS>:<object_name>:<obj key>
    handle = property(*redis_attr('handle'))

    # setup our list of associated objs via their key
    # _key (see above)
    # endpoints is the next piece of the redis key
    # Endpoint is the name of the obj the set's values are IDs for
    endpoints = property(*redis_assoc_set('endpoints',Endpoint))

class RedisEndpoint(Endpoint):
    """
    Endpoint which stores it's data in redis
    """

    object_name = 'Endpoint'
    NS = NS

    def __init__(self):
        self.key = None

    name = property(*redis_attr('name'))
    url = property(*redis_attr('url')
    enabled_events = property(*redis_set('enabled_events'))
    entity = property(*redis_assoc('entity_key',RedisEntity))


