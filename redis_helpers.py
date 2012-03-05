
import redis
rc = redis.Redis('localhost')

from functools import partial

# helper methods for setting attributes on objs
# as redis backed data

# key pattern
# <NS>:<obj name>:<obj key> = {}

def get_key(origin_instance):

    namespace = origin_instance.NS
    # get key for hash
    if namespace:
        key = '%s:'% namespace
    else:
        key = ''
    key += '%s:%s' % (origin_instance.object_name,
                      origin_instance.key)

    return key


def _redis_assoc_setter(hash_key,
                        origin_instance,
                        to_assoc_instance):

    # get our redis key
    key = get_key(origin_instance)

    # set the value
    rc.hset(key,hash_key,to_assoc_instance.key)

def _redis_assoc_getter(hash_key,
                        to_assoc_class,
                        origin_instance):

    # get our redis key
    key = get_key(origin_instance)

    # get the other objects key
    assoc_key = rc.hget(key,hash_key)

    # return an instance of it
    return to_assoc_class.get(assoc_key)


def redis_assoc(hash_key, to_assoc_class):
    """
    returns gettr/settr.
    this is a one to one assoc between the instance and another
    instance.
    """
    return (
        partial(_redis_assoc_getter, hash_key, to_assoc_class),
        partial(_redis_assoc_setter, hash_key)
    )


def _redis_set_setter(key_piece, instance, to_set):

    # get the instance's redis key
    key = get_key(instance)

    # add our piece to the key
    key += ':%s' % key_piece

    # set the set (clearing first)
    pipe = rc.pipeline()
    pipe.delete(key)
    pipe.sadd(key,*to_set)
    pipe.execute()

def _redis_set_getter(key_piece, instance):

    # get the instance's redis key
    key = get_key(instance)

    # add our piece to the key
    key += ':%s' % key_piece

    # return the set
    return rc.smembers(key)

def redis_set(subkey):
    """
    returns gettr/settr
    get / set sets to redis
    """
    return (
        partial(_redis_set_getter, subkey),
        partial(_redis_set_setter, subkey)
    )

def _redis_attr_setter(hash_key, instance, value):
    print 'attr setter: %s %s %s' % (hash_key, instance, value)
    key = get_key(instance)
    return rc.hset(key, hash_key, value)

def _redis_attr_getter(hash_key, instance):
    print 'attr getter: %s %s' % (hash_key, instance)
    key = get_key(instance)
    return rc.hget(key, hash_key)

def redis_attr(hash_key):
    """
    returns getter / setter for backing attr back redis
    hash k/v
    """
    print 'redis attr: %s' % hash_key
    return (
        partial(_redis_attr_getter, hash_key),
        partial(_redis_attr_setter, hash_key)
    )

def _redis_assoc_set_setter(key_piece,
                            instance,
                            to_assoc_instances):
    key = get_key(instance)
    key += ':%s' % key_piece
    pipe = rc.pipeline()
    pipe.delete(key)
    pipe.sadd(key,*[a.key for a in to_assoc_instances])
    pipe.execute()

def _redis_assoc_set_getter(key_piece,
                            to_assoc_class,
                            instance):
    key = get_key(instance)
    key += ':%s' % key_piece
    to_assoc_keys = rc.smembers(key)
    return set(to_assoc_class.get(k) for k in to_assoc_keys if k)

def redis_assoc_set(subkey, to_assoc_class):
    """
    returns gettr/setter
    set / get a list of instance of associated obj
    """
    return (
        partial(_redis_assoc_set_getter, subkey, to_assoc_class),
        partial(_redis_assoc_set_setter, subkey)
    )
