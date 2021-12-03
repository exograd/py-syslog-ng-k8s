class KubernetesParser(object):
    def init(self, options):
        return True

    def deinit(self):
        return True

    def parse(self, message):
        # log message must contains at least 45 characters to be valid.
        if len(message) < 45:
            return False

        level = message[0]
        month = message[1:3]
        day = message[3:5]
        hour = message[6:8]
        minute = message[9:11]
        second = message[12:14]
        sub_second = message[15:21]
        thread = message[22:29].strip()
        i = 30 + message[30:].index(":")
        filename = message[30:i]
        j = i + 1 + message[i+1:].index("]")
        line = message[i+1:j]
        return True
