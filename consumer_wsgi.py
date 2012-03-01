import web
import json


def validate_event_data(event_data):
    """
    returns True if all required fields present, else false
    """
    # only two fields are required, _domain and _name
    valid = True
    required_attrs = ['_domain','_name']
    for attr in required_attrs:
        if not event_data.get(attr):
            valid = False
            break
    return valid

class Handler:
    """
    Handles the event data
    This could mean running something from the command line,
    echoing this event to other consumers, etc
    """

    def __call__(self, event_data, **kwargs):
        print 'HANDLER: %s' % event_data

class WebConsumer:
    """
    Web based application for receiving events.
    Passes event data to event handler
    """

    event_handler = Handler()

    def POST(self,**kwargs):
        try:
            return self.parse_event(post=True,**kwargs)
        except Exception, ex:
            # woops, server error
            web.internalerror()

    def GET(self,**kwargs):
        try:
            return self.parse_event(get=True,**kwargs)
        except Exception, ex:
            # woops, server error
            web.internalerror()

    def parse_event(self,get=False,post=False,**kwargs):

        # get event data (POST or GET)
        event_data = self._get_event_data(get=get,post=post)

        print 'got event data: %s' % event_data

        # validate the event data
        is_valid = validate_event_data(event_data)

        print 'is valid: %s' % is_valid

        # if it's not valid, return an error
        if not is_valid:
            # the data isn't acceptable, 406
            web.notacceptable()

        # now we know we have valid event data
        # pass it off to our handler
        try:
            self.event_handler(event_data, **kwargs)
        except Exception, ex:
            # client error
            web.badrequest()


        # TODO: set our successfull response
        # (the default response will be 200 which is ok)

        return ''

    def _get_event_data(self, get=False, post=False):
        """
        Parses event data from either POST or GET
        returns dictionary of attributes
        """

        # TODO: make more pretty

        # TODO: not have to go through the input
        #       fixing the list part
        to_return = {}

        # see if what we have is json
        if 'json' in web.ctx.env.get('CONTENT_TYPE',''):
            # the POST body should be json
            to_return = json.loads(web.data())

            # since it's json no cleanup needed
            return to_return

        # get the GET / POST params
        single_request_data = web.input()

        # web.py will only use the first value of a multi
        # value set unless the default passed to input is a list
        # to get around this we are going to re-get the input list
        # with all keys defaulting to lists and see if we get back
        # anything values in a list w/ multiple values
        args = dict(((k,[]) for k in single_request_data.keys()))
        multi_value_request_data = web.input(**args)

        # now compare the values from each, if the value in the
        # multi is a list w/ multiple values than we want to keep the multiple
        # values, if it's not we want to remove the list
        srd = single_request_data
        mvrd = multi_value_request_data
        for k,v in mvrd.iteritems():
            if isinstance(v,list) and len(v) == 1:
                to_return[k] = v[0]
            elif isinstance(v,list) and len(v) > 1:
                to_return[k] = v

        # return our inputs
        return to_return


# setup our web.py wsgi app
urls = ('/','WebConsumer')
application = web.application(urls, globals())


if __name__ == '__main__':
    application.run()
