#! /usr/bin/env python3 
# -*- coding: utf-8 -*-

# A script that runs rtl_433 and logs the json results to an sqlite database.
#
# Modifications Copyright 2020 Coburn Wightman
# Removed all database references, added device/data parsing and data events
#
# Modifications Copyright 2017 Ciarán Mooney
# https://github.com/ciaranmooney/rtl_433_2sqlite
#
# Based on the script by Copyright 2014 jcarduino available at:
# https://github/jcarduino/rtl_433_2db
# 

from datetime import datetime
import time
import subprocess
import threading
import queue as Queue
import json

# BEGIN CONFIG
#RTL433 = "/home/ciaran/Code/rtl_433/build/src/rtl_433"
RTL433PATH = "rtl_433"
DEBUG = False 
# END CONFIG

class RtlEventQueue(Queue.Queue):
    def __init__(self, protocol_id, model_name, device_id, parameter_name):
        self.protocol = protocol_id
        self.model_name = model_name
        self.serial_number = device_id
        self.parameter_name = parameter_name
        self.line = None
        super().__init__()
        return
    
    def __repr__(self):
        return '{}: {}, {}, {}'.format(self.parameter_name, self.protocol, self.model_name, self.serial_number)

    def __iter__(self):
        return self
    
    def __next__(self):
        if self.get_next():
            return self
        raise StopIteration

    def get_next(self):
        if self.empty():
            self.line = None
            return False
        self.line = self.get()
        return True

    @property
    def name(self):
        return self.parameter_name

    @property
    def value(self):
        if self.line is None:
            return None
        return self.line[self.parameter_name]

    @property
    def timestamp(self):
        if self.line is None:
            return None
        return self.line['time']

class asyncFileReader(threading.Thread):
    ''' Helper class to implement asynchronous reading of a file
        in a separate thread. Pushes read lines on a queue to
        be consumed in another thread.
    '''

    #def __init__(self, fd, queue, log_file=None):
    def __init__(self, fd, queues, log_file=None):
        #assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self.daemon = True
        self._fd = fd
        self.queues = queues

    def run(self):
        ''' The body of the thread: read rtl lines and put them on the queues.
        '''
        for line in iter(self._fd.readline, ''):
            try:
                line = json.loads(line.decode("utf-8"))
                for queue in self.queues:
                    if queue.model_name not in line['model']:
                        pass
                    elif queue.serial_number is not '' and queue.serial_number != str(line['id']):
                        pass
                    #elif queue.sensor_name is not '' and queue.sensor_name not in line:
                    #    pass
                    elif queue.parameter_name is not '' and queue.parameter_name not in line:
                        pass
                    else:
                        queue.put(line)
                        
            except json.decoder.JSONDecodeError:
                pass


    def eof(self):
        ''' Check whether there is no more content to expect.
        '''
        return not self.is_alive() # and self._queue.empty()


class Rtl433(object):
    ''' concrete class, overrides template functions '''
    def __init__(self):
        self.process = None
        self.stderr_queues = []
        self.stdout_queues = []
        self.protocols = {}
        return

    def create_queue(self, protocol_id, model_name, device_id='', parameter_name=''):
        self.protocols[protocol_id] = True
        queue = RtlEventQueue(protocol_id, model_name, device_id, parameter_name)
        self.stdout_queues.append(queue)
        return queue
    
    def close(self):
        # Let's be tidy and join the threads we've started.
        self.stdout_reader.join()
        self.stderr_reader.join()

        # Close subprocess' file descriptors.
        self.process.stdout.close()
        self.process.stderr.close()
        
    def open(self):
        debug = False
        #debug = True
        rtl_path = RTL433PATH

        protocol_list = []
        for protocol in self.protocols:
            protocol_list.append('-R')
            protocol_list.append(str(protocol))
            
        if debug == False:
            command = [rtl_path, "-F", "json"]
            command.extend(protocol_list)
            #print(command)
            #print("\nStarting RTL433\n")

        if debug == True:
            # possibly better doing this with a test suite.
            # cant just run the shell script because it doesn't output json
            command = [rtl_path, "-G", "-F", "json"]
            print("\nStarting RTL433 - Debug Mode\n")
    
        # Launch the command as subprocess.
        self.process = subprocess.Popen(command, 
                                       stdout=subprocess.PIPE, 
                                       stderr=subprocess.PIPE)

        self.stderr_queues.append(RtlEventQueue(0, 'stderr', '0', '0'))
        self.stderr_reader = asyncFileReader(self.process.stderr, self.stderr_queues)
        self.stderr_reader.start()

        self.stdout_reader = asyncFileReader(self.process.stdout,self.stdout_queues )
        self.stdout_reader.start()

        return True
    
    def eof(self):
        return self.stdout_reader.eof() or self.stderr_reader.eof()

        
    
if __name__ == '__main__':

    rtl = Rtl433()
    #device_protocol = 55 # acurite 606tx sensor
    #temp_queue = rtl.create_queue(device_protocol, '606TX', device_id='58', parameter_name='time') #
    #time_queue = rtl.create_queue(device_protocol, '606TX', device_id='58', parameter_name='temperature_C')

    device_protocol = 74 # acurite 275rm sensor
    temp_queue = rtl.create_queue(device_protocol, '00275rm', device_id='8807', parameter_name='temperature_C')
    #print('new queue {}'.format(temp_queue))
    
    rtl.open()
    print('opened')
    
    while not rtl.eof():
        #while temp_queue.get_next():
        for event in temp_queue:
            print('{}: {} = {}'.format(event.timestamp, event.name, event.value))

        time.sleep(30)

    print('closing')
    rtl.close()
    
    print("Closed.")


