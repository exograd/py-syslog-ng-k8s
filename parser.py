import datetime

class KubernetesParser(object):
    def init(self, options):
        return True

    def deinit(self):
        return True

    def parse(self, message):
        # log message must contains at least 45 characters to be valid.
        if len(message) < 45:
            return False

        try:
            level = message[0]
            # The log message format does not contain year.
            year = datetime.datetime.now().year
            month = int(message[1:3])
            day = int(message[3:5])
            hour = int(message[6:8])
            minute = int(message[9:11])
            second = int(message[12:14])
            microsecond = int(message[15:21])
            thread = int(message[22:29].strip())
            i = 30 + message[30:].index(":")
            filename = message[30:i]
            j = i + 1 + message[i+1:].index("]")
            line = message[i+1:j]

            ts = datetime.datetime(year, month, day,
                                   hour, minute, second, microsecond)
            
            return True
        except ValueError:
            return False
