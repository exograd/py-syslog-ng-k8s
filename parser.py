# Copyright (c) 2021 Bryan Frimin <bryan@frimin.fr>.
#
# Permission to use, copy, modify, and/or distribute this software for
# any purpose with or without fee is hereby granted, provided that the
# above copyright notice and this permission notice appear in all
# copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
# WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE
# AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL
# DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR
# PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
# TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.

import datetime

class KubernetesParser(object):
    def init(self, options):
        """
        Initializes the parser
        """
        self.prefix = options.get("prefix", ".SDATA.kubernetes@32473.")
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
        if len(message) < 30:
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

            metadata = {}
            msg = ""
            if message[j+2] != '"':
                # Message without metadata
                msg = message[j+2:]
            else:
                # Message with metadata
                k = j+3
                while k < len(message):
                    if message[k] == "\\" and message[k+1] == "\"":
                        msg += message[k+1]
                        k += 2
                    elif message[k] == "\"":
                        k += 1
                        break
                    else:
                        msg += message[k]
                        k += 1
                key = ""
                value = ""
                parse_value = False
                while k < len(message):
                    if parse_value == False:
                        l = message[k:].index("=")
                        key = message[k:k+l].strip()
                        k += l + 2
                        parse_value = True
                    else:
                        if message[k] == "\\" and message[k+1] == "\"":
                            value += message[k+1]
                            k += 2
                        elif message[k] == "\"":
                            k += 1
                            metadata[key] = value
                            parse_value = False
                            key = ""
                            value = ""
                        else:
                            value += message[k]
                            k += 1

            for key in metadata:
                log[self.prefix + key] = metadata[key]

            if level == "I":
                log["LEVEL_NUM"] = 5
            elif level == "W":
                log["LEVEL_NUM"] = 4
            elif level == "E":
                log["LEVEL_NUM"] = 3
            elif level == "F":
                log["LEVEL_NUM"] = 2
            else:
                log["LEVEL_NUM"] = 0

            log[self.prefix + "ts"] = ts.isoformat()
            log[self.prefix + "thread"] = thread
            log[self.prefix + "filename"] = filename
            log[self.prefix + "line"] = line
            log['MESSAGE'] = msg

            return True
        except ValueError:
            log['MESSAGE'] = "Cannot parse kubernetes log message: %s" % message
            return False
