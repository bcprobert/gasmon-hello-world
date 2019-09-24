"""
A module consisting of sinks that the processed events will end up at.
"""

from abc import abstractmethod, ABC
from collections import deque, namedtuple
from datetime import datetime
import logging
from time import time
import csv

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Sink(ABC):
    """
    An abstract base class for pipeline sinks.
    """

    @abstractmethod
    def handle(self, events):
        """
        Handle each of the given stream of events.
        """
        pass

    @staticmethod
    def parallel(*sinks):
        return ParallelSink(sinks)


class ParallelSink(Sink):
    def __init__(self, sinks):
        self.sinks = sinks

    def handle(self, events):
        for sink in self.sinks:
            sink.handle(events)


class CalculatesAverage(Sink):
    """
    Averaging_period and expiry_time are in seconds. Will calculate a moving average.
    """

    def __init__(self, averaging_period, expiry_time):
        self.averaging_period_ms = 1000 * averaging_period
        self.expiry_time_ms = 1000 * expiry_time

        # Creates millisecond-size bins for calculating moving averages
        current_time_ms = 1000 * time()
        self.bins = deque(
            [AverageBin(start=(current_time_ms - self.expiry_time_ms), end=(current_time_ms - self.expiry_time_ms), values=[])])

    def handle(self, events):
        for event in events:
            self.add_to_bin(event)
            expired_bin = self.maybe_expire_first_bin_and_get_average()

            if expired_bin is not None:
                average = expired_bin.average
                logger.info(f'Average value for {average.start} to {average.end} is {average.value}')

    def add_to_bin(self, event):
        # Checks if the event is old and should be ignored (i.e. happened in the past)
        if self.bins[0].start > event.timestamp:
            logger.debug(f'Not averaging old event at timestamp {event.timestamp}')
            return

        # Checks if new bins are needed to process the event
        while self.bins[-1].end < event.timestamp:
            current_last_start, current_last_end = self.bins[-1].start, self.bins[-1].end
            logger.debug(
                f'Adding new bin to deal with event at timestamp {event.timestamp} (Current last bucket is {current_last_start} to {current_last_end})')
            self.bins.append(
                AverageBin(start=current_last_end, end=(current_last_end + self.averaging_period_ms), values=[]))

        # Find the correct bin for an event and add the event's value
        bin_index = int(event.timestamp - self.bins[0].start) // self.averaging_period_ms
        self.bins[bin_index].values.append(event.value)
        with open('Gas_Averages.csv', mode='w') as csv_file:
            fieldnames = ['Bin Number', 'Average Value']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for i in range(len(self.bins)):
                if len(self.bins[i].values) != 0:
                    writer.writerow(
                        {'Bin Number': bin_index, 'Average Value': sum(self.bins[i].values)/len(self.bins[i].values)}) # Need to update bin index thing so its correct but averaging is working

    def maybe_expire_first_bin_and_get_average(self):
        current_time_ms = 1000 * time()
        if current_time_ms - self.expiry_time_ms > self.bins[0].end:
            return self.bins.popleft()


class AverageBin(namedtuple('AverageBucket', 'start end values')):

    @property
    def average(self):
        start_datetime = datetime.fromtimestamp(self.start / 1000)
        end_datetime = datetime.fromtimestamp(self.end / 1000)
        average_value = (sum(self.values) / len(self.values)) if self.values else 0
        return Average(start=start_datetime, end=end_datetime, value=average_value)


class Average(namedtuple('Average', 'start end value')):
    """
    A record of the average value of events between two timestamps.
    """