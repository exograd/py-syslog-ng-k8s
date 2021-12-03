import datetime

class KubernetesParser(object):
    def init(self, options):
        """
        Initializes the parser
        """
        self.prefix = options.get("prefix", "kubernetes")
        return True

    def deinit(self):
        """
        Deinitializes the parser, often empty
        """
        return True

    def parse(self, log):
        """
        Parses the log message and returns results
        """
        message = log['MESSAGE'].decode('utf-8')
        # log message must contains at least 45 characters to be valid.
        if len(message) < 45:
            return False

        try:
            level = message[0]
            # The log message format does not contain year. I assume the
            # log message was emit in the current year.
            year = datetime.datetime.now().year
            month = int(message[1:3])
            day = int(message[3:5])
            hour = int(message[6:8])
            minute = int(message[9:11])
            second = int(message[12:14])
            microsecond = int(message[15:21])
            thread = message[22:29].strip()
            i = 30 + message[30:].index(":")
            filename = message[30:i]
            j = i + 1 + message[i+1:].index("]")
            line = message[i+1:j]

            ts = datetime.datetime(year, month, day,
                                   hour, minute, second, microsecond)

            log[self.prefix + "ts"] = ts.isoformat()
            log[self.prefix + "thread"] = thread
            log[self.prefix + "filename"] = filename
            log[self.prefix + "line"] = line
            
            return True
        except ValueError:
            return False
