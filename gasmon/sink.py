"""
A module consisting of sinks that the processed events will end up at.
"""

from abc import abstractmethod, ABC
from collections import deque, namedtuple
from datetime import datetime
import logging
import time
import csv

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt

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
        current_time_ms = 1000 * time.time()
        self.bins = deque(
            [AverageBin(start=(current_time_ms - self.expiry_time_ms), end=(current_time_ms - self.expiry_time_ms),
                        values=[])])

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
        self.write_bins_to_file()

    def write_bins_to_file(self):
        # Write bin values to a csv file so overall intensity trends can be identified
        with open('Gas_Averages.csv', mode='w') as csv_file:
            fieldnames = ['Bin Start', 'Bin End', 'Average Value']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for i in range(len(self.bins)):
                if len(self.bins[i].values) != 0:
                    writer.writerow(
                        {'Bin Start': self.bins[i].average.start,
                         'Bin End': self.bins[i].average.end,
                         'Average Value': self.bins[i].average.value})

    def maybe_expire_first_bin_and_get_average(self):
        current_time_ms = 1000 * time.time()
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


class EventLocation(namedtuple('EventLocation', 'x y value')):
    """
    An event location, consisting of x and y coordinates the intensity of gas at that event.
    """


class LocationAverage(Sink):
    """
    Calculates the weighted average of each location with an event registered there.
    """

    def __init__(self, locations):
        self.locations = locations

    def handle(self, events):
        self.find_event_locations(events)

    def find_event_locations(self, events):
        event_locations = []
        for event in events:
            for location in self.locations:
                if location.id == event.location_id:
                    event_locations.append(EventLocation(
                        x=location.x, y=location.y, value=event.value
                    ))

        self.plot_results(event_locations)

    @staticmethod
    def plot_results(event_locations):
        x_points = []
        y_points = []
        values = []
        for i in range(len(event_locations)):
            x_points.append(event_locations[i].x)
            y_points.append(event_locations[i].y)
            values.append(event_locations[i].value)

        fig = plt.figure()
        ax = fig.gca(projection='3d')

        surf = ax.plot_trisurf(x_points, y_points, values, cmap='viridis', linewidth=0, antialiased=True)

        fig.colorbar(surf)
        ax.set_xlabel(r'x')
        ax.set_ylabel(r'y')
        ax.set_zlabel(r'Value')
        ax.set_title('GasMon')

        plt.show()
