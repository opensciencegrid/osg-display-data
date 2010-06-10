
import os
import time

class Data(object):

    def __init__(self, cp):
        self.cp = cp
        self.num_sites = 0
        self.num_jobs = 0
        self.num_users = 0
        self.num_ces = 0
        self.num_ses = 0
        self.transfer_volume_mb = 0
        self.num_transfers = 0
        self.transfer_volume_rate = 0
        self.transfer_rate = 0
        self.jobs_rate = 0
        self.total_hours = 0
        # Historical numbers
        self.num_jobs_hist = 0
        self.total_hours_hist = 0
        self.num_transfers_hist = 0
        self.transfer_volume_mb_hist = 0

    def set_num_transfers_hist(self, num):
        self.num_transfers_hist = num
            
    def set_num_jobs_hist(self, num):
        self.num_jobs_hist = num
    
    def set_transfer_volume_mb_hist(self, mb):
        self.transfer_volume_mb_hist = mb
        
    def set_total_hours_hist(self, hours):
        self.total_hours_hist = hours

    def set_num_sites(self, num_sites):
        self.num_sites = num_sites

    def set_num_jobs(self, num_jobs):
        self.num_jobs = num_jobs

    def set_num_users(self, num_users):
        self.num_users = num_users

    def set_num_ces(self, num_ces):
        self.num_ces = num_ces

    def set_num_ses(self, num_ses):
        self.num_ses = num_ses

    def set_num_transfers(self, num_transfers):
        self.num_transfers = num_transfers

    def set_transfer_volume_mb(self, transfer_volume_mb):
        self.transfer_volume_mb = transfer_volume_mb

    def set_jobs_rate(self, jobs_rate):
        self.jobs_rate = jobs_rate

    def set_transfer_volume_rate(self, transfer_volume_rate):
        self.transfer_volume_rate = transfer_volume_rate

    def set_transfer_rate(self, transfer_rate):
        self.transfer_rate = transfer_rate

    def set_total_hours(self, hours_sum):
        self.total_hours = hours_sum

    def run(self, fp):
        info = {}
        info['time'] = int(time.time())
        info['num_transfers_hist'] = int(self.num_transfers_hist)
        info['num_jobs_hist'] = int(self.num_jobs_hist)
        info['total_hours_hist'] = int(self.total_hours_hist)
        info['transfer_volume_mb_hist'] = int(self.transfer_volume_mb_hist)
        info['num_jobs'] = int(self.num_jobs)
        info['num_users'] = int(self.num_users)
        info['num_sites'] = int(self.num_sites)
        info['num_ces'] = int(self.num_ces)
        info['num_ses'] = int(self.num_ses)
        info['num_transfers'] = int(self.num_transfers)
        info['transfer_volume_mb'] = int(self.transfer_volume_mb)
        info['transfer_volume_rate'] = float(self.transfer_volume_rate)
        info['transfer_rate'] = float(self.transfer_rate)
        info['jobs_rate'] = float(self.jobs_rate)
        info['total_hours'] = float(self.total_hours)
        fp.write(str(info))
        fp.flush()
        os.fsync(fp)
        fp.close()

