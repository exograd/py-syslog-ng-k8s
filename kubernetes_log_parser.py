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

        self.prefix = options.get('prefix', '.SDATA.kubernetes@32473.')
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
        INIT = 0
        MESSAGE = 1
        QUOTED_MESSAGE = 2
        METADATA = 3
        METADATA_KEY = 4
        METADATA_VALUE = 5
        METADATA_QUOTED_VALUE = 6
        METADATA_UNQUOTED_VALUE = 7
        NEXT_METADATA = 8

        QUOTE = '"'
        SLASH = '\\'
        SPACE = ' '
        EQUAL = '='

        message = log['MESSAGE'].decode('utf-8')
        level = message[0]
        if level != 'I' or level != 'W' or level != 'E' or level != 'F':
            log['MESSAGE'] = message
            return True

        year = datetime.datetime.now().year
        month = int(message[1:3])
        day = int(message[3:5])
        hour = int(message[6:8])
        minute = int(message[9:11])
        second = int(message[12:14])
        microsecond = int(message[15:21])
        thread = message[22:29].strip()
        i = 30 + message[30:].index(':')
        filename = message[30:i]
        j = i + 1 + message[i + 1:].index(']')
        line = message[i + 1:j]

        ts = datetime.datetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            )

        metadata = {}
        msg = ''
        k = j + 2
        state = INIT
        current_key = ''

        while k < len(message):
            if state == INIT:
                if message[k] == QUOTE:
                    state = QUOTED_MESSAGE
                else:
                    state = MESSAGE
                k += 1
            elif state == MESSAGE:
                msg += message[k]
                k += 1
            elif state == QUOTED_MESSAGE:
                if message[k] == SLASH and message[k + 1] == QUOTE:
                    msg += message[k + 1]
                    k += 1
                elif message[k] == QUOTE:
                    state = METADATA
                else:
                    msg += message[k]
                k += 1
            elif state == METADATA:
                if message[k] == ' ':
                    k += 1
                else:
                    state = METADATA_KEY
            elif state == METADATA_KEY:
                if message[k] == EQUAL:
                    metadata[current_key] = ''
                    state = METADATA_VALUE
                else:
                    current_key += message[k]
                k += 1
            elif state == METADATA_VALUE:
                if message[k] == QUOTE:
                    state = METADATA_QUOTED_VALUE
                    k += 1
                else:
                    state = METADATA_UNQUOTED_VALUE
            elif state == METADATA_QUOTED_VALUE:
                if message[k] == SLASH and message[k + 1] == QUOTE:
                    metadata[current_key] += message[k + 1]
                    k += 1
                elif message[k] == QUOTE:
                    state = NEXT_METADATA
                else:
                    metadata[current_key] += message[k]
                k += 1
            elif state == METADATA_UNQUOTED_VALUE:
                if message[k] == SPACE:
                    state = NEXT_METADATA
                else:
                    metadata[current_key] += message[k]
                k += 1
            elif state == NEXT_METADATA:
                current_key = ''
                state = METADATA

        for key in metadata:
            log[self.prefix + key] = metadata[key]

        if level == 'I':
            log[self.prefix + 'level'] = '5'
        elif level == 'W':
            log[self.prefix + 'level'] = '4'
        elif level == 'E':
            log[self.prefix + 'level'] = '3'
        elif level == 'F':
            log[self.prefix + 'level'] = '2'
        else:
            log[self.prefix + 'level'] = '0'

        log[self.prefix + 'ts'] = ts.isoformat()
        log[self.prefix + 'thread'] = thread
        log[self.prefix + 'filename'] = filename
        log[self.prefix + 'line'] = line
        log['MESSAGE'] = msg

        return True
