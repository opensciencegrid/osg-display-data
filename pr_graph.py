
import os
import sys
import sets
import time
import shutil
import signal
import urllib2
import datetime
import tempfile
import optparse
import ConfigParser

from xml.dom.minidom import parse

import Image
import MySQLdb
import matplotlib.figure
import matplotlib.backends.backend_svg
import matplotlib.backends.backend_agg

from matplotlib.pylab import setp
from mpl_toolkits.axes_grid.parasite_axes import HostAxes, ParasiteAxes

dpi = 72


class OIMDataSource(object):

    def __init__(self, cp):
        self.cp = cp

    resource_group_url = 'http://myosg.grid.iu.edu/rgsummary/xml?datasource=' \
        'summary&all_resources=on&gridtype=on&gridtype_1=on&active=on&' \
        'active_value=1&disable=on&disable_value=0&' \
        'summary_attrs_showhierarchy=on&summary_attrs_showservice=on'

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
        return sites

    def query_ce_se(self):
        
        fd = urllib2.urlopen(self.resource_group_url)
        dom = parse(fd)
        ses = sets.Set()
        ces = sets.Set()
        for service_dom in dom.getElementsByTagName("Services"):
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
        self.conn = MySQLdb.connect(user=user, passwd=password, host=host,
            port=port, db=database)

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
        return [i[1] for i in curs.fetchall()]

    def query_user_num(self):
        curs = self.conn.cursor()
        params = self.get_params()
        curs.execute(self.user_num_query, params)
        return curs.fetchall()[0][0]

    def query_jobs(self):
        curs = self.conn.cursor()
        params = self.get_params()
        curs.execute(self.jobs_query, params)
        return [i[1]/float(params['span']/60) for i in curs.fetchall()]

    jobs_query = """
        SELECT
          (truncate((unix_timestamp(ServerDate)-%(offset)s)/%(span)s, 0)*%(span)s) as time,
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
            (truncate((unix_timestamp(ServerDate)-%(offset)s)/%(span)s, 0)*%(span)s) as time,
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
        self.time = None
        self.count = None
        self.volume_mb = None


def get_files(cp, name):
    name = cp.get("Filenames", name)
    fd, tmpname = tempfile.mkstemp(prefix="osg_display")
    os.close(fd)
    return name, tmpname

def commit_files(name, tmpname):
    shutil.move(tmpname, name)


class DataSourceTransfers(object):

    def __init__(self, cp):
        self.cp = cp
        self.data = {}
        
    def run(self):
        self.connect()
        self.load_cached()
        self.determine_missing()
        
    def connect(self):
        user = self.cp.get("Gratia", "User")
        password = self.cp.get("Gratia", "Password")
        host = self.cp.get("Gratia", "Host")
        database = self.cp.get("Gratia", "Database")
        port = int(self.cp.get("Gratia", "Port"))
        self.conn = MySQLdb.connect(user=user, passwd=password, host=host,
            port=port, db=database)

    def load_cached(self):
        try:
            data = pickle.load(open(self.cp.get("Filenames", "transfers_data"),
                "r"))
            # Verify we didn't get useless data
            for time, tdata in data.items():
                assert isinstance(time, datetime.datetime)
                assert isinstance(tdata, TransferData)
                assert isinstance(tdata.time, datetime.datetime)
                assert tdata.count != None
                assert tdata.volume_mb != None
            self.data = data
        except:
            pass

    def save_cache(self):
        now = datetime.datetime.now()
        old_keys = []
        for key in self.data.keys():
            if (now - key).days >= 2:
                old_keys.append(key)
        for key in old_keys:
            del self.data
        try:
            name, tmpname = get_files(self.cp, "transfers_data")
            fp = open(tmpname, 'w')
            pickle.dump(self.data, fp)
            fp.close()
            commit_files(name, tmpname)
        except:
            pass

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
        return [i[1] for i in curs.fetchall()]

    transfers_query = """
        SELECT
          (truncate((unix_timestamp(ServerDate))/%(span)s, 0)*%(span)s) as time,
          sum(Value*SU.Multiplier) as Records
        FROM JobUsageRecord_Meta JURM
        JOIN JobUsageRecord JUR on JURM.dbid=JUR.dbid
        JOIN Network N on (JUR.dbid = N.dbid)
        JOIN SizeUnits SU on N.StorageUnit = SU.Unit
        WHERE
          ResourceType="Storage" AND
          ServerDate >= %(starttime)s AND
          ServerDate < %(endtime)s
        GROUP BY time;
        """


class PRGraph(object):

    def __init__(self, cp, graph_num):
        self.cp = cp
        self.num = graph_num

    def build_canvas(self):
        ylabel = self.cp.get("Labels", "YLabel%i" % self.num)
        if False:
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
        ax_rect = (.11, 0.06, .89, .85)
        ax = fig.add_axes(ax_rect)
        frame = ax.patch
        frame.set_fill(False)
        ax.grid(True, color='#555555', linewidth=1)

        ax.set_frame_on(False)
        ax.get_xaxis().set_visible(False)
        ylabel = ax.set_ylabel(ylabel)
        ylabel.set_size(int(self.cp.get("Sizes", "YLabelSize")))
        setp(ax.get_yticklabels(), size=int(self.cp.get("Sizes", "YTickSize")))
        setp(ax.get_ygridlines(), linestyle='-')
        setp(ax.get_yticklines(), visible=False)

        self.ax = ax
        self.canvas = canvas
        self.fig = fig

    def write_graph(self):
        self.canvas.draw()
        if False:
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
        legend = self.ax.legend(loc=9, mode="expand",
            bbox_to_anchor=(0.5, 1.02, 1., .102))
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

    def run(self, fp):
        info = {}
        info['time'] = int(time.time())
        info['num_jobs'] = int(self.num_jobs)
        info['num_users'] = int(self.num_users)
        info['num_sites'] = int(self.num_sites)
        info['num_ces'] = int(self.num_ces)
        info['num_ses'] = int(self.num_ses)
        fp.write(str(info))
        fp.flush()
        fp.close()


def configure():
    usage = "usage: %prog -c config_file"
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", help="PR Graph config file",
        dest="config")
    opts, args = parser.parse_args()

    if not opts.config:
        parser.print_help()
        print "\nMust pass a config file."
        sys.exit(1)

    cp = ConfigParser.ConfigParser()
    cp.readfp(open(opts.config, "r"))
    return cp

def main():
    cp = configure()

    # Set the alarm in case if we go over time
    signal.alarm(int(cp.get("Settings", "timeout")))

    # Generate the graphs
    ds = DataSource(cp)
    ds.run()
    pr = PRGraph(cp, 1)
    pr.data = ds.query_jobs()
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

    # Generate the JSON
    ods = OIMDataSource(cp)
    prd = PRData(cp)
    prd.set_num_sites(len(ods.query_sites()))
    prd.set_num_jobs(num_jobs)
    prd.set_num_users(ds.query_user_num())
    ces, ses = ods.query_ce_se()
    prd.set_num_ces(ces)
    prd.set_num_ses(ses)

    name, tmpname = get_files(cp, "json")
    fd = open(tmpname, 'w')
    prd.run(fd)
    commit_files(name, tmpname)

if __name__ == '__main__':
    main()

