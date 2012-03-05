from lib.revent import ReventClient
from producer import Producer


# build an app which has a web UI / API for managing
# subscriptions, gets it's events from revent, and fully
# supports the eventedapi.org spec

NS = 'revent_broadcaster'

class SubscriptionHandler(Thread):

    timeout = 60
    # after receiving a bad response from the client
    # how long do we wait before trying the event agian
    retry_frequency = 60 * 1 # = 1 min
    # how long should we sleep if the client
    # throws an error ?
    fail_sleep_time = 60.0 * .5 # 30 seconds

    def __init__(self, is_stopping,
                       subscription_key, consumer_url,
                       enabled_events, domain):

        # respect
        Thread.__init__(self)

        # flag whether we are shutting down
        self.is_stopping = is_stopping

        # details of the events we're sending out
        self.subscription_key = subscription_key
        self.consumer_url = consumer_url
        self.domain = domain
        self.producer = Producer(self.consumer_url,
                                 self.domain)

        # get the details of our subscription
        self.enabled_events = enabled_events

    def run(self):
        """
        sit on the revent queue for the subscription
        passing events as HTTP(S) requests
        """

        # loop waiting for event data
        while not self.is_stopping.is_set():

            # try and get an event
            event_data = self.get_event()

            # if didn't get one just go back around
            if event_data is None:
                continue

            # we got an event, broadcast it to our consumer
            status_code = None
            try:
                status_code = self.producer.send_event(data=event_data)
            except Exception, ex:
                # woops, the event will be requeued
                print 'exception sending event: %s' % ex

            # check the status code

            # if it's 410 (gone) than we want to remove the subscriber
            if status_code and status_code == 410:
                # will cancel retry as part of removing
                self.remove_subscription()

            # we want to retry if we get a 500 series error
            elif status_code and status_code in (500,503,504):
                # TODO: respect 503 retry after times
                # take no action and we'll retry after the timeout
                self.fail_sleep()

            # a 200 is success, verify that we've send the msg
            elif status_code and 200 <= status_code < 300:
                self.verify_event_processed(event_name, event_data)

            # if we had an unknown exception (didn't get a status code)
            # than we'll let it re-queue
            elif not status_code:
                pass

            # if the status code didn't get caught already
            # than we don't want to retry. Not sure what the best action
            # would be here, maybe wait a super long retry time to give
            # them recovery time?
            else:
                self.cancel_retry(event_name, event_data)
                self.fail_sleep()


    def get_event(self):
        """
        get the next event from the queue
        """
        raise NotImplementedError

    def remove_subscription(self):
        """
        Updates details for subscription on shared storage
        as being done. Also clears outstanding events,
        broadcasts that our subscription's state has changed
        """

        # remove our subscription details
        self.remove_subscription_details()

        # let the other broadcaster's know our details have changed
        self.broadcast_state_change()

    def remove_subscription_details(self):
        """
        removes our details from shared storage
        """
        raise NotImplementedError

    def broadcast_state_change(self):
        """
        broadcasts on redis pubsub that our subscription details have changed
        """
        raise NotImplementedError

    def verify_event_processed(self, event_name, event_data):
        """
        notifies that we have finished processing the event and it
        should not be requeued
        """
        raise NotImplementedError

    def cancel_retry(self, event_name, event_data):
        """
        makes sure the event we are currently handling does
        not get requeued
        """
        raise NotImplementedError

    def fail_sleep(self):
        """
        after failing to make a request we want
        to sleep for a bit
        """

        if not self.is_stopping.is_set():
            sleep(self.fail_sleep_time)

        # make the fail sleep time longer so that if we fail again
        # the next attempt will be later
        self.fail_sleep_time *= 2

        return True

class ReventSubscriptionHandler(SubscriptionHandler):
    """
    watches revent event queue for given subscriber
    and pushes events to their end point
    """

    event_verify_timeout = 3 * 60 # 3min

    def __init__(self, *args):

        SubscriptionHandler.__init__(self, *args)

        # redis client
        self.rc = None

        # setup our revent client, we want msgs which
        # we don't mark handled to be re-introduced to
        # the queue.
        # channel key is subscription key, auto create channel
        self.revent = ReventClient(subscription_key,
                                   self.revent_filter_string,
                                   True, True,
                                   self.event_verify_timeout)


    def run(self):
        self.rc = redis.Redis(REDIS_HOST)
        SubscriptionHandler.run(self)

    def get_revent_filter_string(self):
        """
        builds a revent regex string for event names
        based on the watched events for this subscription
        """

        # the regex is going to be OR'd together event names
        regex_string = '|'.join(self.enabled_events)
        return regex_string

    # setup the filter string as a property
    revent_filter_string = property(get_revent_filter_string)

    def get_event(self):
        """
        attemps to get the next event, blocking.
        returns event_data or None
        """
        event_name, event_data = revent.get_event(block=True,
                                                  timeout=self.timeout)

        # add the name to the event data if it's not present
        if not '_name' in event_data:
            event_data['_name'] = event_name

        return event_data

    def remove_subscription(self):
        """
        removes our subscription for events, broadcasts that
        our subscription state has changed
        """

        # respect
        SubscriptionHandler.remove_subscription(self)

        # tell revent to remove our event channel
        revent.remove_channel()

    def remove_subscription_details(self):

        # remove our subscription details from redis
        key = '%s:subscription:%s:enabled_events' % (NS,self.subscription_key)
        rc.del(KEY)

    def broadcast_state_change(self):
        """
        broadcasts on redis pubsub that our subscription details have changed
        """

        rc.publish('%s:subscription_change' % NS,
                   self.subscription_key)

    def verify_event_processed(self, event_name, event_data):
        """
        notifies that we have finished processing the event and it
        should not be requeued
        """
        return revent.verify_msg(event_name, event_data)

    def cancel_retry(self, event_name, event_data):
        """
        makes sure the event we are currently handling does
        not get requeued
        """

        # to cancel a retry we'll just mark the event handled
        return self.verify_event_processed(event_name, event_data)


class Broadcaster:
    """
    broadcasts events to consumers
    """

    # we are going to use redis to track our subscribers

    # when we start up we are going to go through the subscriber
    # keys and populate our subscriber lookup

    # we are going to have one thread per subscriber
    # a thread which is going to wait for broadcasts about
    #  subscriber changes

    sleep_time = 2

    SubscriptionChangeListener = SubscriptionChangeListener
    SubscriptionHandler = SubscriptionHandler

    def __init__(self, domain='defaultdomain'):

        # what domain are the events broadcast from?
        self.domain = domain

        # lookup of subscriber threads
        # since there is one subscriber thread per endpoint
        # the key is the endpoint key and the value is the
        # subscriber handler
        self.subscriber_lookup = {}

        # flag so that we can co-ordinate threads stopping
        self.is_stopping = Event()

        # our queue for alerting threads to subscription change
        self.subscription_change_queue = Queue()

        # our thread which listenes for broadcasts alerting
        # us to subscriber detail changes
        self.subscription_change_thread = None

    def start_subscription_change_listener(self):
        """
        starts a thread which will listen for broadcasts
        alerting us to subscriber changes
        """

        try:

            self.subscription_change_thread = \
                    self.SubscriptionChangeListener(
                        self.is_stopping,
                        self.subscription_change_queue)

            self.subscription_change_thread.start()

        except Exception, ex:
            print 'Exception starting change listener thread: %s' % ex
            self.is_stopping.set()


    def sync_subscription_handlers(self):
        """
        bring our subscription handlers in to line w/ the
        shared data store
        """

        # there is going to be a subscription for each endpoint
        # start off by going through all the endpoints
        for endpoint in self.get_endpoints():

            handler = self.subscription_lookup.get(endpoint.key)

            # if we don't have a handler, we need to start one
            if not handler:
                self.create_handler(endpoint)

            # if we have a handler, check it's properties
            # against what our endpoint's data says
            elif handler.consumer_url != endpoint.url or \
                 handler.enabled_events = endpoint.enabled_events:

                # kill the handler off
                self.destroy_handler(endpoint.key, handler)

                # start it again
                self.create_handler(endpoint)


    def destroy_handler(self, key, handler):
        """
        stops the handler and removes it from the lookup
        """
        handler.is_stopping.set()
        handler.join()
        del self.subscription_lookup[key]
        return True

    def create_handler(self, endpoint):
        """
        creates a new handler for the endpoint
        """
        is_stopping = Event()

        # create a handler for the subscription
        handler = self.SubscriptionHandler(is_stopping,
                                           endpoint.key,
                                           endpoint.url,
                                           endpoint.enabled_events,
                                           self.domain)

        # add it to the lookup
        self.subscription_lookup[endpoint.key] = handler
        return handler

    def run(self):
        """
        starts server, threads and all
        """

        # setup a thread for handling subscription change broadcasts
        self.start_subscription_change_listener()

        # spin up handlers for each of our subscriptions
        self.sync_subscription_handlers()

        # now put the master thread in a loop
        try:
            while not self.is_stopping.is_set():
                sleep(self.sleep_time)
        except KeyboardInterrupt, ex:
            pass
        except Exception, ex:
            print 'Top Exception: %s' % ex

        finally:
            # stop all our threads
            self.stop_subscription_change_listener()
            self.stop_subscription_handlers()


    def stop_subscription_handlers(self):
        # stop all our handlers
        print 'stopping handlers'
        for key, handler in self.subscription_lookup.iteritems():
            try:
                handler.is_stopping.set()
                handler.join()
            except Exception, ex:
                log.exception('Stoppiong handler: %s' % handler.key)

    def stop_subscription_change_listener(self):
        self.subscription_change_listener.is_stopping.set()
        # force kill if it's still listening (blocking socket)
        try:
            self.subscription_change_listener._Thread__stop()
        except Exception, ex:
            log.exception('Stopping subscription change listener')


class SubscriptionChangeListener(Thread):

    """
    listens for subscription change broadcasts,
    puts the details on the change queue
    """

    def __init__(self, is_stopping, change_queue):
        self.change_queue = change_queue
        self.is_stopping = is_stopping
        Thread.__init__(self)

    def run(self):
        """
        blocks, listening to subscription change broadcasts
        and adding change details to queue
        """

        while not self.is_stopping.is_set():

            # _listen method will block until it gets a msg
            for msg_data in self._listen_subscription_change():

                # get the subscription key from the msg
                subscription_key = msg_data.get('data')

                # put the details on our queue
                self.change_queue.push(subscription_key)

    def _listen_subscription_change(self):
        # do this yourself
        raise NotImplementedError

class RedisSubscriptionChangeListener(SubscriptionChangeListener):

    def __init__(self, *args, **kwargs):
        SubscriptionChangeListener.__init__(self,*args,**kwargs)

        # our redis clients
        self.rc = None
        self.rc_pubsub = None

    def run(self):
        self.rc = redis.Redis(REDIS_HOST)
        self.rc_pubsub = self.rc.pubsub()
        self.rc_pubsub.subscribe('%s:subscription_change' % NS)
        SubscriptionChangeListener.run(self)

    def _listen_subscription_change(self):
        return self.rcps.listen()

