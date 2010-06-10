
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

    def get_params(self):
        hours = int(int(self.cp.get("Gratia", "hours"))*1.5)
        now = int(time.time()-60)
        prev = now - 3600*hours
        offset = prev % 3600
        starttime = datetime.datetime(*time.gmtime(prev)[:6])
        endtime = datetime.datetime(*time.gmtime(now)[:6])
        return {'offset': offset, 'starttime': starttime, 'endtime': endtime,
            'span': 3600}

    def query_users(self):
        curs = self.conn.cursor()
        params = self.get_params()
        curs.execute(self.users_query, params)
        results = [i[1] for i in curs.fetchall()]
        log.info("Gratia returned %i results for users" % len(results))
        log.debug("Results are: %s." % ", ".join([str(i) for i in results]))
        return results

    def query_user_num(self):
        curs = self.conn.cursor()
        params = self.get_params()
        curs.execute(self.user_num_query, params)
        results = curs.fetchall()[0][0]
        log.info("Gratia returned %i active users." % results)
        return results

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

    users_query = """
        SELECT
          time, count(*)
        FROM (
          SELECT
            (truncate((unix_timestamp(ServerDate)-%(offset)s)/%(span)s, 0)*
               %(span)s) as time,
             JUR.CommonName as Users
          FROM JobUsageRecord_Meta JURM
          JOIN JobUsageRecord JUR on JURM.dbid=JUR.dbid
          WHERE
            ServerDate >= %(starttime)s AND
            ServerDate < %(endtime)s
          GROUP BY time, JUR.CommonName
        ) as foo
        GROUP BY time
        """

    user_num_query = """
        SELECT
          count(*)
        FROM (
          SELECT
            distinct(JUR.CommonName)
          FROM JobUsageRecord_Meta JURM
          JOIN JobUsageRecord JUR on JURM.dbid=JUR.dbid
          WHERE
              ServerDate >= %(starttime)s AND
              ServerDate < %(endtime)s
        ) as foo
        """


class HistoricalDataSource(DataSource):

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

    def get_params(self):
        months = int(int(self.cp.get("Gratia", "months"))+2)
        end = datetime.datetime(*(list(time.gmtime()[:2]) + [1,0,0,0]))
        start = end - datetime.timedelta(14*31, 0)
        start -= datetime.timedelta(start.day-1, 0)
        return {'starttime': start, 'endtime': end}

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
            log.debug("Week starting on %s: Transfers %i, Transfer PB %.2f" % \
                (i[0], count, mbs/1024**2))
        count_results = [i[0] for i in all_results]
        hour_results = [i[1] for i in all_results]
        num_results = int(self.cp.get("Gratia", "months"))
        count_results = count_results[-num_results-1:-1]
        hour_results = hour_results[-num_results-1:-1]
        self.disconnect()
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

