import requests

class Producer:
    """
    Facilitates sending events to consumers
    """

    def __init__(self, url=None, domain=None, method="POST", send_json=False):
        """
        Allow setting default values
        """
        self.url = url
        self.domain = domain
        self.method = method
        self.send_json = send_json

    def send_event(self, url=None, data={}, name=None, domain=None,
                         method="POST", send_json=False):
        """
        Sends an HTTP event, leans on defaults from init
        """

        r = self._send_event(url or self.url,
                             data, name,
                             domain or self.domain,
                             method or self.method,
                             True if (send_json or self.send_json) else False)
        return r

    @staticmethod
    def _send_event(url, data={}, name=None, domain=None,
                    method="POST", send_json=False):
        """
        Sends an event to consumer using either POST or GET
        """

        # make sure we have all the data we need
        assert url, "URL Required"
        assert domain or data.get('_domain'), "Domain Required"
        assert name or data.get('_name'), "Name Required"
        assert method.lower() in ('get','post'), "Method must be GET or POST"

        # setup our event data
        data = data or {}
        if domain:
            data['_domain'] = domain
        if name:
            data['_name'] = name

        headers = {}

        # if we are posting we can use JSON
        if method.lower() == 'POST':
            if send_json:
                data = json.dumps(data)
                headers['Content-Type'] = 'application/json'
            else:
                headers['Content-Type'] = 'application/x-www-form-urlencoded'

        # make our HTTP request
        r = getattr(requests,method.lower())(url, data=data, headers=headers)

        # return the response status, let them take action
        return r.status_code
