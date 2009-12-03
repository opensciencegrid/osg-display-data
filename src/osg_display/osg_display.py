
import os
import sys
import sets
import time
import pickle
import shutil
import signal
import logging
import urllib2
import datetime
import tempfile
import optparse
import ConfigParser

from xml.dom.minidom import parse

import Image
import MySQLdb
import matplotlib
import matplotlib.figure
import matplotlib.font_manager
import matplotlib.backends.backend_svg
import matplotlib.backends.backend_agg

from matplotlib.pylab import setp
from mpl_toolkits.axes_grid.parasite_axes import HostAxes, ParasiteAxes

dpi = 72
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)
_euid = os.geteuid()

class OIMDataSource(object):

    def __init__(self, cp):
        self.cp = cp

    resource_group_url = 'http://myosg.grid.iu.edu/rgsummary/xml?datasource=' \
        'summary&all_resources=on&gridtype=on&gridtype_1=on&active=on&' \
        'active_value=1&disable=on&disable_value=0&' \
        'summary_attrs_showhierarchy=on&summary_attrs_showservice=on' \
        '&service=on&service_1=on&service_5=on&service_2=on&service_3=on'

    def query_sites(self):
        fd = urllib2.urlopen(self.resource_group_url)
        dom = parse(fd)
        sites = sets.Set()
        for site_dom in dom.getElementsByTagName("Site"):
            for name_dom in site_dom.getElementsByTagName("Name"):
                try:
                    sites.add(str(name_dom.firstChild.data))
                except:
                    pass
        log.debug("OIM returned the following sites: %s" % ", ".join(sites))
        log.info("OIM has %i registered sites." % len(sites))
        return sites

    def query_ce_se(self):
        log.debug("Querying the following MyOSG URL: %s" % \
            self.resource_group_url)
        fd = urllib2.urlopen(self.resource_group_url)
        dom = parse(fd)
        ses = sets.Set()
        ces = sets.Set()
        for service_dom in dom.getElementsByTagName("Service"):
            service_type = None
            for name_dom in service_dom.getElementsByTagName("Name"):
                try:
                    service_type = str(name_dom.firstChild.data).strip()
                except:
                    pass
            uri = None
            for uri_dom in service_dom.getElementsByTagName("ServiceUri"):
                try:
                    uri = str(uri_dom.firstChild.data).strip()
                except:
                    pass
            if uri and service_type:
                if service_type == 'SRMv2':
                    ses.add(uri)
                elif service_type == 'CE':
                    ces.add(uri)
        log.debug("OIM returned the following CEs: %s." % ", ".join(ces))
        log.debug("OIM returned the following SEs: %s." % ", ".join(ses))
        log.info("OIM returned %i CEs and %i SEs" % (len(ces), len(ses)))
        return len(ces), len(ses)


class DataSource(object):

    def __init__(self, cp):
        self.cp = cp

    def run(self):
        self.connect()

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
        hours = int(self.cp.get("Gratia", "hours"))
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
        results = [i[1]/float(params['span']/60) for i in curs.fetchall()]
        log.info("Gratia returned %i results for jobs" % len(results))
        log.debug("Results are: %s." % ", ".join([str(i) for i in results]))
        return results

    jobs_query = """
        SELECT
          (truncate((unix_timestamp(ServerDate)-%(offset)s)/%(span)s, 0)*
            %(span)s) as time,
          count(*) as Records
        FROM JobUsageRecord_Meta 
        WHERE
          ServerDate >= %(starttime)s AND
          ServerDate < %(endtime)s
        GROUP BY time;
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


class TransferData(object):

    def __init__(self):
        self.starttime = None
        self.endtime = None
        self.count = None
        self.volume_mb = None


def get_files(cp, config_name):
    name = cp.get("Filenames", config_name)
    try:
        name = name % {'uid': _euid}
    except:
        raise
    fd, tmpname = tempfile.mkstemp(prefix="osg_display")
    os.close(fd)
    log.debug("Using temporary file %s for %s" % (tmpname, config_name))
    return name, tmpname

def commit_files(name, tmpname):
    log.debug("Overwriting %s with %s." % (name, tmpname))
    try:
        os.chmod(tmpname, 0644)
        shutil.move(tmpname, name)
    except Exception, e:
        log.exception(e)
        log.error("Unable to overwrite old file %s." % name)
        raise


class DataSourceTransfers(object):

    def __init__(self, cp):
        self.cp = cp
        self.data = {}
        self.missing = sets.Set()
        
    def run(self):
        self.connect()
        self.load_cached()
        self.determine_missing()
        self.query_missing()
        
    def connect(self):
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

    def load_cached(self):
        try:
            data = pickle.load(open(self.cp.get("Filenames", "transfer_data") \
                % {'uid': _euid}, "r"))
            # Verify we didn't get useless data
            for time, tdata in data.items():
                assert isinstance(time, datetime.datetime)
                assert isinstance(tdata, TransferData)
                assert isinstance(tdata.starttime, datetime.datetime)
                assert isinstance(tdata.endtime, datetime.datetime)
                assert tdata.count != None
                assert tdata.volume_mb != None
                assert tdata.starttime != None
            self.data = data
            log.info("Successfully loaded transfer data from cache; %i" \
                " cache entries." % len(data))
        except Exception, e:
            log.warning("Unable to load cache; it may not exist. Error: %s" % \
               str(e))

    def save_cache(self):
        now = datetime.datetime.now()
        old_keys = []
        for key in self.data.keys():
            if (now - key).days >= 2:
                old_keys.append(key)
        for key in old_keys:
            del self.data[key]
        try:
            name, tmpname = get_files(self.cp, "transfer_data")
            fp = open(tmpname, 'w')
            pickle.dump(self.data, fp)
            fp.close()
            commit_files(name, tmpname)
            log.debug("Saved data to cache.")
        except Exception, e:
            log.warning("Unable to write cache; message: %s" % str(e))

    def _timestamp_to_datetime(self, ts):
        return datetime.datetime(*time.gmtime(ts)[:6])

    def determine_missing(self):
        now = time.time()
        hour_now = now - (now % 3600)
        if (now-hour_now) < 15*60:
            hour_now -= 3600
        self.missing.add(self._timestamp_to_datetime(hour_now))
        cur = hour_now
        hours = int(self.cp.get("Gratia Transfer", "hours"))
        while cur >= now - hours*3600:
            cur -= 3600
            cur_dt = self._timestamp_to_datetime(cur)
            if cur_dt not in self.data:
                self.missing.add(cur_dt)

    def query_missing(self):
        now = time.time()
        log.info("Querying %i missing data entries." % len(self.missing))
        for mtime in self.missing:
            starttime = mtime
            endtime = mtime + datetime.timedelta(0, 3600)
            results = self.query_transfers(starttime, endtime)
            if not results:
                log.warning("No transfer results found for %s." % starttime)
            for result in results:
                res_time, count, volume_mb = result
                res_time = float(res_time)
                starttime = self._timestamp_to_datetime(res_time)
                if now-res_time >= 3600:
                    endtime = self._timestamp_to_datetime(res_time+3600)
                else:
                    endtime = self._timestamp_to_datetime(now)
                if res_time > now:
                    continue
                td = TransferData()
                td.starttime = starttime
                td.endtime = endtime
                td.count = count
                td.volume_mb = volume_mb
                self.data[starttime] = td
                log.debug("Successfully parsed results for %s." % starttime)
                self.save_cache()

    def query_transfers(self, starttime, endtime):
        log.info("Querying Gratia Transfer DB for transfers from %s to %s." \
            % (starttime.strftime("%Y-%m-%d %H:%M:%S"),
            endtime.strftime("%Y-%m-%d %H:%M:%S")))
        curs = self.conn.cursor()
        params = {'span': 3600}
        params['starttime'] = starttime
        params['endtime'] = endtime
        curs.execute(self.transfers_query, params)
        return curs.fetchall()

    def get_data(self):
        all_times = self.data.keys()
        all_times.sort()
        all_times = all_times[-24:]
        results = []
        for time in all_times:
            results.append((self.data[time].count, self.data[time].volume_mb))
        return results

    def get_volume_rates(self):
        all_times = self.data.keys()
        all_times.sort()
        all_times = all_times[-24:]
        results = []
        for time in all_times:
            td = self.data[time]
            interval = td.endtime - td.starttime
            interval_s = interval.days*86400 + interval.seconds
            results.append(td.volume_mb/interval_s)
        return results

    def get_rates(self):
        all_times = self.data.keys()
        all_times.sort()
        all_times = all_times[-24:]
        results = []
        for time in all_times:
            td = self.data[time]
            interval = td.endtime - td.starttime
            interval_s = interval.days*86400 + interval.seconds
            results.append(td.count/float(interval_s))
        return results


    transfers_query = """
        SELECT
          (truncate((unix_timestamp(ServerDate))/%(span)s, 0)*%(span)s) as time,
          count(*) as Records,
          sum(Value*SU.Multiplier) as SizeMB
        FROM JobUsageRecord_Meta JURM
        JOIN Network N on (JURM.dbid = N.dbid)
        JOIN SizeUnits SU on N.StorageUnit = SU.Unit
        WHERE
          ServerDate >= %(starttime)s AND
          ServerDate < %(endtime)s
        GROUP BY time;
        """


class PRGraph(object):

    def __init__(self, cp, graph_num):
        self.cp = cp
        self.num = graph_num
        self.svg = self.cp.get("Settings", "output_svg").lower() != "false"
        font_list = [i.strip() for i in self.cp.get("Settings", "Font").\
            split(",")]
        #if 'font.sans-serif' in matplotlib.rcParams:
        #    font_list = font_list + matplotlib.rcParams['font.sans-serif']
        matplotlib.rcParams['font.family'] = 'sans-serif'
        matplotlib.rcParams['font.sans-serif'] = font_list
        fm = matplotlib.font_manager.FontManager()
        prop = matplotlib.font_manager.FontProperties()
        self.legend = self.cp.get("Labels", "Legend").lower() == "true"

    def build_canvas(self):
        ylabel = self.cp.get("Labels", "YLabel%i" % self.num)
        if self.svg:
            FigureCanvas = matplotlib.backends.backend_svg.FigureCanvasSVG
        else:
            FigureCanvas = matplotlib.backends.backend_agg.FigureCanvasAgg

        fig = matplotlib.figure.Figure()
        canvas = FigureCanvas(fig)
        height = int(self.cp.get("Sizes", "height"))
        height_inches = height/float(dpi)
        width = int(self.cp.get("Sizes", "width"))
        width_inches = width/float(dpi)
        self.height, self.width = height, width
        fig.set_size_inches(width_inches, height_inches)
        fig.set_dpi(dpi)
        fig.set_facecolor('white')
        if self.legend:
            ax_rect = (.11, 0.06, .89, .85)
        else:
            label_len = len(ylabel)
            extra = label_len*.006
            if label_len > 5:
                extra += .01
            if label_len < 5:
                extra -= .025
            ax_rect = (.09+extra, 0.06, .91-extra, .90)
        ax = fig.add_axes(ax_rect)
        frame = ax.patch
        frame.set_fill(False)
        ax.grid(True, color='#555555', linewidth=1)

        ax.set_frame_on(False)
        ax.get_xaxis().set_visible(False)
        if ylabel:
            ylabel = ax.set_ylabel(ylabel)
            ylabel.set_size(int(self.cp.get("Sizes", "YLabelSize")))
            ylabel.set_rotation("horizontal")
        setp(ax.get_yticklabels(), size=int(self.cp.get("Sizes", "YTickSize")))
        setp(ax.get_ygridlines(), linestyle='-')
        setp(ax.get_yticklines(), visible=False)

        self.ax = ax
        self.canvas = canvas
        self.fig = fig

    def write_graph(self):
        self.canvas.draw()
        if self.svg:
            renderer = matplotlib.backends.backend_svg.RendererSVG( \
                self.width, self.height, self.file)
            self.canvas.figure.draw(renderer)
            renderer.finalize()
        else:
            size = self.canvas.get_renderer().get_canvas_width_height()
            buf = self.canvas.tostring_argb()
            im = Image.fromstring("RGBA", (int(size[0]), int(size[1])), buf,
                "raw", "RGBA", 0, 1)
            a, r, g, b = im.split()
            im = Image.merge("RGBA", (r, g, b, a))
            im.save(self.file, format="PNG")

    def draw(self):
        data_len = len(self.data)
        X = range(data_len)
        color = self.cp.get("Colors", "Line%i" % self.num)
        line = self.ax.plot(X, self.data, marker="o", markeredgecolor=color,
            markerfacecolor="white", markersize=13, linewidth=5,
            markeredgewidth=5, color=color, label=self.cp.get("Labels",
            "Legend%i" % self.num))
        max_ax = max(self.data)*1.1
        self.ax.set_ylim(-0.5, max_ax)
        self.ax.set_xlim(-1, data_len)

        if self.legend:
            legend = self.ax.legend(loc=9, mode="expand",
                bbox_to_anchor=(0.25, 1.02, 1., .102))
            setp(legend.get_frame(), visible=False)
            setp(legend.get_texts(), size=int(self.cp.get("Sizes", "LegendSize")))

    def parse_data(self):
        pass

    def run(self, fp):
        self.file = fp
        self.parse_data()
        self.build_canvas()
        self.draw()
        self.write_graph()
        fp.close()


class PRData(object):

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

    def run(self, fp):
        info = {}
        info['time'] = int(time.time())
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
        fp.write(str(info))
        fp.flush()
        fp.close()


def configure():
    usage = "usage: %prog -c config_file"
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", help="PR Graph config file",
        dest="config", default="/etc/osg_display/osg_display.conf")
    parser.add_option("-q", "--quiet", help="Reduce verbosity of output",
        dest="quiet", default=False, action="store_true")
    parser.add_option("-d", "--debug", help="Turn on debug output",
        dest="debug", default=False, action="store_true")
    opts, args = parser.parse_args()

    if not opts.config:
        parser.print_help()
        print "\nMust pass a config file."
        sys.exit(1)

    log.handlers = []

    if not opts.quiet:
        handler = logging.StreamHandler(sys.stdout)
        log.addHandler(handler)

    for handler in log.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - " \
            "%(message)s")
        handler.setFormatter(formatter)

    if opts.debug:
        log.setLevel(logging.DEBUG)

    log.info("Reading from log file %s." % opts.config)

    cp = ConfigParser.SafeConfigParser()
    cp.readfp(open(opts.config, "r"))

    logging.basicConfig(filename=cp.get("Settings", "logfile"))

    return cp

def main():
    cp = configure()

    # Set the alarm in case if we go over time
    timeout = int(cp.get("Settings", "timeout"))
    signal.alarm(timeout)
    log.debug("Setting script timeout to %i." % timeout)

    # Generate the graphs
    ds = DataSource(cp)
    ds.run()
    pr = PRGraph(cp, 1)
    jobs_data = ds.query_jobs()
    pr.data = jobs_data
    num_jobs = sum(pr.data)*60

    name, tmpname = get_files(cp, "jobs")
    fd = open(tmpname, 'w')
    pr.run(fd)
    commit_files(name, tmpname)

    pr = PRGraph(cp, 2)
    pr.data = ds.query_users()
    name, tmpname = get_files(cp, "users")
    fd = open(tmpname, 'w')
    pr.run(fd)
    commit_files(name, tmpname)

    # Generate the more-complex transfers graph
    dst = DataSourceTransfers(cp)
    dst.run()
    pr = PRGraph(cp, 3)
    pr.data = [i/1024. for i in dst.get_volume_rates()]
    log.debug("Transfer rates: %s" % ", ".join([str(i) for i in pr.data]))
    name, tmpname = get_files(cp, "transfers")
    fd = open(tmpname, 'w')
    pr.run(fd)
    commit_files(name, tmpname)
    transfer_data = dst.get_data()
    num_transfers = sum([i[0] for i in transfer_data])
    transfer_volume_mb = sum([i[1] for i in transfer_data])

    # Generate the JSON
    ods = OIMDataSource(cp)
    prd = PRData(cp)
    prd.set_num_sites(len(ods.query_sites()))
    prd.set_num_jobs(num_jobs)
    prd.set_num_users(ds.query_user_num())
    ces, ses = ods.query_ce_se()
    prd.set_num_ces(ces)
    prd.set_num_ses(ses)
    prd.set_num_transfers(num_transfers)
    prd.set_transfer_volume_mb(transfer_volume_mb)
    prd.set_transfer_volume_rate([i for i in dst.get_volume_rates()][-1])
    prd.set_jobs_rate(jobs_data[-1]/60.)
    prd.set_transfer_rate([i for i in dst.get_rates()][-1])

    name, tmpname = get_files(cp, "json")
    fd = open(tmpname, 'w')
    prd.run(fd)
    commit_files(name, tmpname)

if __name__ == '__main__':
    main()

