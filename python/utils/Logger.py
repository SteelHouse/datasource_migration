import datetime
import sys
import traceback
from enum import Enum

from python.utils.config import config


class Logger:
    def __init__(self):
        log_config = config('../config.ini', 'logging')
        self.log_file = log_config['log_file']
        self.enable_traceback = log_config['enable_traceback']
        level = log_config['debug_level']
        match level:
            case 'DEBUG':
                self.logging_level = LogLevel.DEBUG
            case 'INFO':
                self.logging_level = LogLevel.INFO
            case 'WARNING':
                self.logging_level = LogLevel.WARNING
            case 'WARN':
                self.logging_level = LogLevel.WARNING
            case 'ERROR':
                self.logging_level = LogLevel.ERROR
            case _:
                self.logging_level = LogLevel.INFO

    def debug(self, message):
        if self.logging_level >= LogLevel.DEBUG:
            self.log(message, LogLevel.DEBUG)

    def info(self, message):
        if self.logging_level >= LogLevel.INFO:
            self.log(message, LogLevel.INFO)

    def warning(self, message):
        if self.logging_level >= LogLevel.WARNING:
            self.log(message, LogLevel.WARNING)

    def error(self, message):
        if self.logging_level >= LogLevel.ERROR:
            self.log(message, LogLevel.ERROR)

    def log(self, message, level):
        log_message = str(datetime.datetime.now()) + ' ' + str(level.name) + ' ' + message + '\n'
        if self.log_file:
            with open(self.log_file, 'a') as f:
                f.write(log_message)
        else:
            if level == LogLevel.ERROR:
                print(log_message, file=sys.stderr)
                if self.enable_traceback:
                    traceback.print_exc()
            else:
                print(log_message)

    def print_logging_level(self):
        print(self.logging_level)


class LogLevel(int, Enum):
    DEBUG = 4
    INFO = 3
    WARNING = 2
    ERROR = 1
