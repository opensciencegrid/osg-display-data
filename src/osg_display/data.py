
import os
import time

class Data(object):

    def __init__(self, cp):
        self.cp = cp
        self.data_sources = []

    def add_datasource(self, data_source):
        self.data_sources.append(data_source.get_json())

    def run(self, fp):
        info = {}
        info['time'] = int(time.time())
        for ds in self.data_sources:
            info.update(ds)
        fp.write(str(info).replace("'", '"').replace('\\"', "'").replace("\\'", "'").replace(': None', ': ""'))
        fp.flush()
        os.fsync(fp)
        fp.close()

