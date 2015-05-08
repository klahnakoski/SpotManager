


class FakeAWSConnection(object):
    """
     A TREMENDOUSLY POOR IMPLEMENTATION OF boto's AWS CONNECTION
     JUST ENOUGH TO BE USED BY TEST CASES
    """

    def __init__(self):
        self.spot_requests = []
        self.instances = []
        self.volumes = []



