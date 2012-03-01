
import web
import consumer_wsgi
import redis
import json

rc = redis.Redis('127.0.0.1')

NS = 'web_bridge'

def get_redis_key(queue_key):
    """
    returns the redis key for the given queue
    """
    return '%s:queue:%s' % (NS, queue_key)

class RedisEventBufferer(consumer_wsgi.Handler):
    """
    When new event data is received this handler will
    buffer into redis queue
    """

    def push_to_queue(self, event_data, queue_key):

        # json encode our event data
        print 'dumping: %s' % event_data
        event_data_string = json.dumps(event_data)

        # get our Q's key, and add our event data to queue
        key = get_redis_key(queue_key)
        print 'pushing to queue: %s %s' % (key,event_data_string)
        rc.rpush(key, event_data_string)

    def __call__(self, event_data, queue_key):
        """
        take the event data and put it into the consumer's
        queue
        """
        print 'called event buffer handler: %s %s' % (
                event_data, queue_key)
        self.push_to_queue(event_data, queue_key)


class EventConsumer(consumer_wsgi.WebConsumer):
    """
    WSGI application for consuming events, buffering them,
    and than feeding them back out to clients.
    """

    event_handler = RedisEventBufferer()

    def POST(self, queue_key='DEFAULT'):
        # over ride to deal w/ args
        print 'Consumer POST: %s' % queue_key
        return consumer_wsgi.WebConsumer.POST(self,
                                              queue_key=queue_key)

    def GET(self, queue_key='DEFAULT'):
        # over ride to deal w/ args
        print 'Consumer GET: %s' % queue_key
        return consumer_wsgi.WebConsumer.GET(self,
                                             queue_key=queue_key)

class EventFeeder:
    """
    WSGI app which will feed the events from the queue
    back to the web client
    """

    def GET(self, queue_key='DEFAULT'):

        print 'Feeder GET: %s' % queue_key

        # get the next event data from the queue
        # we are getting back the json string
        event_data = self.get_event_data(queue_key,decode=False)

        # if we didn't receive event data we are going
        # to return a 304 not modified
        if not event_data:
            print 'no queue msg, nothing found'
            return web.notfound()

        # let the client know we're piping json
        web.header('Content-Type','application/json')

        # if we did get the event data we are going
        # to return it as a json string
        print 'got event data: %s' % event_data
        return event_data

    def get_event_data(self, queue_key, decode=True):
        """
        Returns next event on queue if any
        """

        # try and get the next msg from the queue
        key = get_redis_key(queue_key)
        print 'checking redis: %s' % key
        msg = rc.lpop(key)

        if msg and decode:
            return json.loads(msg)

        # none if there was nothing in the list
        return msg


# setup the wsgi app
urls = (
    '/push/(.*)', 'EventConsumer',
    '/pull/(.*)', 'EventFeeder'
)
application = web.application(urls, globals())


if __name__ == '__main__':
    application.run()
