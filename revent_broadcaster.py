from lib.revent import ReventClient
from producer import Producer


# build an app which has a web UI / API for managing
# subscriptions, gets it's events from revent, and fully
# supports the eventedapi.org spec

NS = 'revent_broadcaster'


class SubscriptionHandler:
    """
    watches revent event queue for given subscriber
    and pushes events to their end point
    """

    event_verify_timeout = 3 * 60 # 3min
    timeout = 60
    retry_frequency = 60 * 5 # = 5min

    def __init__(self, is_stopping,
                       subscription_key, consumer_url, domain):

        # flag whether we are shutting down
        self.is_stopping = is_stopping

        # details of the events we're sending out
        self.subscription_key = subscription_key
        self.consumer_url = consumer_url
        self.domain = domain
        self.producer = Producer(self.consumer_url,
                                 self.domain)

        # how long should we sleep if the client
        # throws an error ?
        self.fail_sleep_time = 60 * 1 # 1 min

        # flag whether we should be removed from pool
        self.dead = False

        # get the details of our subscription
        self.watched_events = []
        self.update_subscription_details()

        # setup our revent client, we want msgs which
        # we don't mark handled to be re-introduced to
        # the queue.
        # channel key is subscription key, auto create channel
        self.revent = ReventClient(subscription_key,
                                   self.revent_filter_string,
                                   True, True,
                                   self.event_verify_timeout)

    def update_subscription_details(self):
        """
        hits redis to get our details
        """
        key = '%s:subscription:%s:watched_events' % (NS,self.subscription_key)
        self.watched_events = rc.lrange(key,0,-1)

    def get_revent_filter_string(self):
        """
        builds a revent regex string for event names
        based on the watched events for this subscription
        """

        # the regex is going to be OR'd together event names
        regex_string = '|'.join(self.watched_events)
        return regex_string

    # setup the filter string as a property
    revent_filter_string = property(get_revent_filter_string)

    def start(self):
        """
        sit on the revent queue for the subscription
        passing events as HTTP(S) requests
        """

        # loop waiting for event data
        while not self.is_stopping.is_set():

            # try and get an event
            event_name, event_data = revent.get_event(timeout=self.timeout)

            # if didn't get one just go back around
            if event_name is None:
                continue

            # we got an event, broadcast it to our consumer
            status_code = None
            try:
                status_code = self.producer.send_event(data=event_data,
                                                       name=event_name)
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

    def remove_subscription(self):
        """
        removes our subscription for events, marks self as dead
        """

        # tell revent to remove our event channel
        revent.remove_channel()

        # remove our subscription details from redis
        key = '%s:subscription:%s:watched_events' % (NS,self.subscription_key)
        rc.del(key)

        # let the other broadcaster's know our details have changed
        self.server.broadcast_subscriber_change(self.subscription_key)

        return True

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


class Broadcaster:
    """
    broadcasts revent events to consumers
    """

    # we are going to use redis to track our subscribers

    # when we start up we are going to go through the subscriber
    # keys and populate our subscriber lookup

    # we are going to have one thread per subscriber
    # a thread which is going to wait for broadcasts about
    #  subscriber changes

    def __init__(self, domain='defaultdomain'):

        # what domain are the events broadcast from?
        self.domain = domain

        # lookup of subscriber keys
        self.subscriber_lookup = {}

        # flag so that we can co-ordinate threads stopping
        self.is_stopping =


    def run(self):
        """
        starts server, threads and all
        """

        # setup a thread for handling subscription change broadcasts
        self.start_subscription_change_listener()

        # spin up handlers for each of our subscriptions
        self.sync_subscription_handlers()

        # now put the master thread in a loop
