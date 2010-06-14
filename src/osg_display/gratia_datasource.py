
import time
import MySQLdb
import datetime

from common import log

class DataSource(object):

    def __init__(self, cp):
        self.cp = cp

    def run(self):
        self.connect()

    def disconnect(self):
        self.conn.close()

    def connect(self):
        user = self.cp.get("Gratia", "User")
        password = self.cp.get("Gratia", "Password")
        host = self.cp.get("Gratia", "Host")
        database = self.cp.get("Gratia", "Database")
        port = int(self.cp.get("Gratia", "Port"))
        try:
            self.conn = MySQLdb.connect(user=user, passwd=password, host=host,
                port=port, db=database)
            log.info("Successfully connected to Gratia database")
        except Exception, e:
            log.exception(e)
            log.error("Unable to connect to Gratia database")
            raise
        curs = self.conn.cursor()
        curs.execute("set time_zone='+0:00'")

    def connect_transfer(self):
        user = self.cp.get("Gratia Transfer", "User")
        password = self.cp.get("Gratia Transfer", "Password")
        host = self.cp.get("Gratia Transfer", "Host")
        database = self.cp.get("Gratia Transfer", "Database")
        port = int(self.cp.get("Gratia Transfer", "Port"))
        try:
            self.conn = MySQLdb.connect(user=user, passwd=password, host=host,
                port=port, db=database)
        except Exception, e:
            log.exception(e)
            log.error("Unable to connect to Gratia Transfer DB")
            raise
        curs=self.conn.cursor()
        curs.execute("set time_zone='+0:00'")


class HourlyJobsDataSource(DataSource):
    """
    Hourly view of the Gratia job data
    """

    def __init__(self, cp):
        super(HourlyJobsDataSource, self).__init__(cp)
        self.count_results = None
        self.hour_results = None

    def get_params(self):
        hours = int(int(self.cp.get("Gratia", "hours"))*1.5)
        now = int(time.time()-60)
        prev = now - 3600*hours
        offset = prev % 3600
        starttime = datetime.datetime(*time.gmtime(prev)[:6])
        endtime = datetime.datetime(*time.gmtime(now)[:6])
        return {'offset': offset, 'starttime': starttime, 'endtime': endtime,
            'span': 3600}

    def get_json(self):
        assert self.count_results != None
        assert self.hour_results != None
        num_jobs = sum(self.count_results)
        total_hours = sum(self.hour_results)
        return {'jobs_hourly': int(num_jobs), 'cpu_hours_hourly': float(total_hours)}

    def query_jobs(self):
        curs = self.conn.cursor()
        params = self.get_params()
        curs.execute(self.jobs_query, params)
        results = curs.fetchall()
        all_results = [(i[1], i[2]) for i in results]
        log.info("Gratia returned %i results for jobs" % len(all_results))
        log.debug("Job result dump:")
        for i in results:
            count, hrs = i[1:]
            time_tuple = time.gmtime(i[0])
            time_str = time.strftime("%Y-%m-%d %H:%M", time_tuple)
            log.debug("Time %s: Count %i, Hours %.2f" % (time_str, count, hrs))
        count_results = [i[0] for i in all_results]
        hour_results = [i[1] for i in all_results]
        num_results = int(self.cp.get("Gratia", "hours"))
        count_results = count_results[-num_results-1:-1]
        hour_results = hour_results[-num_results-1:-1]
        self.count_results, self.hour_results = count_results, hour_results
        return count_results, hour_results

    jobs_query = """
        SELECT
          (truncate((unix_timestamp(JUR.EndTime)-%(offset)s)/%(span)s, 0)*
            %(span)s) as time,
          count(*) as Records,
          sum(WallDuration)/3600 as Hours
        FROM JobUsageRecord_Meta 
        JOIN JobUsageRecord JUR ON JUR.dbid=JobUsageRecord_Meta.dbid
        WHERE
          ServerDate >= %(starttime)s AND
          ServerDate < %(endtime)s AND
          JUR.EndTime < %(endtime)s
        GROUP BY time
        ORDER BY time ASC
        """


class MonthlyDataSource(DataSource):

    def get_params(self):
        months = int(int(self.cp.get("Gratia", "months"))+2)
        end = datetime.datetime(*(list(time.gmtime()[:2]) + [1,0,0,0]))
        start = end - datetime.timedelta(14*31, 0)
        start -= datetime.timedelta(start.day-1, 0)
        return {'starttime': start, 'endtime': end}

    def get_json(self):
        assert self.count_results != None
        assert self.hour_results != None
        assert self.transfer_results != None
        assert self.transfer_volume_results != None
        num_jobs = sum(self.count_results)
        total_hours = sum(self.hour_results)
        total_transfers = sum(self.transfer_results)
        total_transfer_volume = sum(self.transfer_volume_results)
        return {'jobs_monthly': int(num_jobs), 'cpu_hours_monthly': \
            float(total_hours), 'transfers_monthly': float(total_transfers),
            'transfer_volume_mb_monthly': total_transfer_volume}

    def query_jobs(self):
        curs = self.conn.cursor()
        params = self.get_params()
        curs.execute(self.jobs_query, params)
        results = curs.fetchall()
        all_results = [(i[1], i[2]) for i in results]
        log.info("Gratia returned %i results for jobs" % len(all_results))
        log.debug("Job result dump:")
        for i in results:
            count, hrs = i[1:]
            log.debug("Month starting on %s: Jobs %i, Job Hours %.2f" % (i[0],
                count, hrs))
        count_results = [i[0] for i in all_results]
        hour_results = [i[1] for i in all_results]
        num_results = int(self.cp.get("Gratia", "months"))
        count_results = count_results[-num_results-1:-1]
        hour_results = hour_results[-num_results-1:-1]
        self.count_results, self.hour_results = count_results, hour_results
        return count_results, hour_results

    def query_transfers(self):
        self.connect_transfer()
        curs = self.conn.cursor()
        params = self.get_params()
        curs.execute(self.transfers_query, params)
        results = curs.fetchall()
        all_results = [(i[1], i[2]) for i in results]
        log.info("Gratia returned %i results for transfers" % len(all_results))
        log.debug("Transfer result dump:")
        for i in results:
            count, mbs = i[1:]
            log.debug("Month starting on %s: Transfers %i, Transfer PB %.2f" % \
                (i[0], count, mbs/1024**2))
        count_results = [i[0] for i in all_results]
        hour_results = [i[1] for i in all_results]
        num_results = int(self.cp.get("Gratia", "months"))
        count_results = count_results[-num_results-1:-1]
        hour_results = hour_results[-num_results-1:-1]
        self.disconnect()
        self.transfer_results = count_results
        self.transfer_volume_results = hour_results
        return count_results, hour_results

    jobs_query = """
        SELECT
          MIN(EndTime) AS time,
          SUM(Njobs) AS Records,
          SUM(WallSeconds)/3600 AS Hours
        FROM (
            SELECT
              ProbeName,
              R.VOCorrid as VOCorrid,
              MIN(EndTime) AS EndTime,
              SUM(Njobs) AS NJobs,
              SUM(WallDuration*Cores) AS WallSeconds,
              YEAR(EndTime) as Y,
              MONTH(EndTime) as M
            FROM MasterSummaryData R FORCE INDEX(index02)
            WHERE
              EndTime >= %(starttime)s AND
              EndTime < %(endtime)s AND
              ResourceType = 'Batch'
            GROUP BY Y, M, ProbeName, VOCorrid
          ) as R
        JOIN Probe P on R.ProbeName = P.probename
        JOIN Site S on S.siteid = P.siteid
        JOIN VONameCorrection VC ON (VC.corrid=R.VOcorrid)
        JOIN VO on (VC.void = VO.void)
        WHERE
          S.SiteName NOT IN ('NONE', 'Generic', 'Obsolete') AND
          VO.VOName NOT IN ('unknown', 'other')
        GROUP BY Y, M
        ORDER BY time ASC
    """

    transfers_query = """
        SELECT
          MIN(time) AS time,
          SUM(Records) as Records,
          SUM(TransferSize*SizeUnits.Multiplier) AS MB
        FROM ( 
            SELECT 
              ProbeName,
              MIN(StartTime) AS time,
              SUM(Njobs) AS Records,
              sum(TransferSize) AS TransferSize,
              R.StorageUnit,
              YEAR(StartTime) as Y,
              MONTH(StartTime) as M
            FROM MasterTransferSummary R FORCE INDEX(index02)
            WHERE StartTime>= %(starttime)s AND StartTime< %(endtime)s
            GROUP BY Y, M, R.StorageUnit, ProbeName
          ) as R
        JOIN Probe P ON R.ProbeName = P.probename
        JOIN Site S ON S.siteid = P.siteid
        JOIN SizeUnits on (SizeUnits.Unit = R.StorageUnit)
        WHERE
          S.SiteName NOT IN ('NONE', 'Generic', 'Obsolete')
        GROUP BY Y,M
        ORDER BY time ASC
    """

class DailyDataSource(DataSource):
    """
    Data source to provide transfer and job information over the past 30
    days.  Queries the Gratia summary tables for jobs and transfers.
    """

    def get_params(self):
        days = int(int(self.cp.get("Gratia", "days"))+2)
        end = datetime.datetime(*(list(time.gmtime()[:3]) + [0,0,0]))
        start = end - datetime.timedelta(days, 0)
        start -= datetime.timedelta(start.day-1, 0)
        return {'starttime': start, 'endtime': end}

    def get_json(self):
        assert self.count_results != None
        assert self.hour_results != None
        assert self.transfer_results != None
        assert self.transfer_volume_results != None
        num_jobs = sum(self.count_results)
        total_hours = sum(self.hour_results)
        num_transfers = int(sum(self.transfer_results))
        transfer_volume = float(sum(self.transfer_volume_results))
        return {'jobs_daily': int(num_jobs), 'cpu_hours_daily': \
            float(total_hours), 'transfers_daily': num_transfers,
            'transfer_volume_mb_daily': transfer_volume}

    def query_jobs(self):
        curs = self.conn.cursor()
        params = self.get_params()
        curs.execute(self.jobs_query, params)
        results = curs.fetchall()
        all_results = [(i[1], i[2]) for i in results]
        log.info("Gratia returned %i results for daily jobs" % len(all_results))
        log.debug("Job result dump:")
        for i in results:
            count, hrs = i[1:]
            log.debug("Day %s: Jobs %i, Job Hours %.2f" % (i[0],
                count, hrs))
        count_results = [i[0] for i in all_results]
        hour_results = [i[1] for i in all_results]
        num_results = int(self.cp.get("Gratia", "days"))
        count_results = count_results[-num_results-1:-1]
        hour_results = hour_results[-num_results-1:-1]
        self.count_results, self.hour_results = count_results, hour_results
        return count_results, hour_results

    def query_transfers(self):
        self.connect_transfer()
        curs = self.conn.cursor()
        params = self.get_params()
        curs.execute(self.transfers_query, params)
        results = curs.fetchall()
        all_results = [(i[1], i[2]) for i in results]
        log.info("Gratia returned %i results for daily transfers" % \
            len(all_results))
        log.debug("Transfer result dump:")
        for i in results:
            count, mbs = i[1:]
            log.debug("Day %s: Transfers %i, Transfer PB %.2f" % \
                (i[0], count, mbs/1024**2))
        count_results = [i[0] for i in all_results]
        hour_results = [i[1] for i in all_results]
        num_results = int(self.cp.get("Gratia", "days"))
        count_results = count_results[-num_results-1:-1]
        hour_results = hour_results[-num_results-1:-1]
        self.disconnect()
        self.transfer_results, self.transfer_volume_results = count_results, \
            hour_results
        return count_results, hour_results

    jobs_query = """
        SELECT
          Date,
          SUM(Njobs) AS Records,
          SUM(WallSeconds)/3600 AS Hours
        FROM (
            SELECT
              ProbeName,
              R.VOCorrid AS VOCorrid,
              SUM(Njobs) AS NJobs,
              SUM(WallDuration*Cores) AS WallSeconds,
              DATE(EndTime) AS Date
            FROM MasterSummaryData R FORCE INDEX(index02)
            WHERE
              EndTime >= %(starttime)s AND
              EndTime < %(endtime)s AND
              ResourceType = 'Batch'
            GROUP BY Date, ProbeName, VOCorrid
          ) as R
        JOIN Probe P on R.ProbeName = P.probename
        JOIN Site S on S.siteid = P.siteid
        JOIN VONameCorrection VC ON (VC.corrid=R.VOcorrid)
        JOIN VO on (VC.void = VO.void)
        WHERE
          S.SiteName NOT IN ('NONE', 'Generic', 'Obsolete') AND
          VO.VOName NOT IN ('unknown', 'other')
        GROUP BY Date
        ORDER BY Date ASC
    """

    transfers_query = """
        SELECT
          Date,
          SUM(Records) as Records,
          SUM(TransferSize*SizeUnits.Multiplier) AS MB
        FROM ( 
            SELECT 
              ProbeName,
              SUM(Njobs) AS Records,
              sum(TransferSize) AS TransferSize,
              R.StorageUnit,
              DATE(StartTime) as Date
            FROM MasterTransferSummary R FORCE INDEX(index02)
            WHERE
              StartTime >= %(starttime)s AND
              StartTime < %(endtime)s
            GROUP BY Date, R.StorageUnit, ProbeName
          ) as R
        JOIN Probe P ON R.ProbeName = P.probename
        JOIN Site S ON S.siteid = P.siteid
        JOIN SizeUnits on (SizeUnits.Unit = R.StorageUnit)
        WHERE
          S.SiteName NOT IN ('NONE', 'Generic', 'Obsolete')
        GROUP BY Date
        ORDER BY Date ASC
    """

