
import os
import sys
import sets
import time
import urllib2
import datetime
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
        'active_value=1&disable=on&disable_value=0&summary_attrs_showhierarchy=on' \
        '&summary_attrs_showservice=on'

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

class PRGraph(object):

    def __init__(self, cp):
        self.cp = cp

    def build_canvas(self):
        ylabel1 = self.cp.get("Labels", "YLabel1")
        ylabel2 = self.cp.get("Labels", "YLabel2")
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
        fig.set_size_inches(width_inches, height_inches)
        fig.set_dpi(dpi)
        fig.set_facecolor('white')
        ax_rect = (.11, 0.06, .78, .85)
        #ax = HostAxes(fig, ax_rect)
        #ax2 = ParasiteAxes(ax, sharex=ax)
        ax = fig.add_axes(ax_rect)
        ax2 = ax.twinx()
        frame = ax.patch
        frame.set_fill(False)
        ax.grid(True, color='#555555', linewidth=1)

        ax.set_frame_on(False)
        ax.get_xaxis().set_visible(False)
        ylabel = ax.set_ylabel(ylabel1)
        ylabel.set_size(int(self.cp.get("Sizes", "YLabelSize")))
        ylabel = ax2.set_ylabel(ylabel2)
        ylabel.set_size(int(self.cp.get("Sizes", "YLabelSize")))
        setp(ax.get_yticklabels(), size=int(self.cp.get("Sizes", "YTickSize")))
        setp(ax2.get_yticklabels(), size=int(self.cp.get("Sizes", "YTickSize")))
        setp(ax.get_ygridlines(), linestyle='-')
        setp(ax.get_yticklines(), visible=False)
        setp(ax2.get_yticklines(), visible=False)

        self.ax1 = ax
        self.ax2 = ax2
        self.canvas = canvas
        self.fig = fig

    def write_graph(self):
        self.canvas.draw()
        if False:
            renderer = matplotlib.backends.backend_svg.RendererSVG( \
                width, height, self.file)
            self.canvas.figure.draw(renderer)
            renderer.finish()
        else:
            size = self.canvas.get_renderer().get_canvas_width_height()
            buf = self.canvas.tostring_argb()
            im = Image.fromstring("RGBA", (int(size[0]), int(size[1])), buf,
                "raw", "RGBA", 0, 1)
            a, r, g, b = im.split()
            im = Image.merge("RGBA", (r, g, b, a))
            im.save(self.file, format="PNG")

    def draw(self):
        data_len = len(self.data1)
        X = range(data_len)
        color1 = self.cp.get("Colors", "Line1")
        color2 = self.cp.get("Colors", "Line2")
        line1 = self.ax1.plot(X, self.data1, marker="o", markeredgecolor=color1,
            markerfacecolor="white", markersize=13, linewidth=5,
            markeredgewidth=5, color=color1, label=self.cp.get("Labels", "Legend1"))
        line2 = self.ax2.plot(X, self.data2, marker="o", markeredgecolor=color2,
            markerfacecolor="white", markersize=13, linewidth=5,
            markeredgewidth=5, color=color2, label=self.cp.get("Labels", "Legend2"))
        max_ax1 = max(self.data1)*1.1
        max_ax2 = max(self.data2)*1.1
        self.ax1.set_ylim(-0.5, max_ax1)
        self.ax1.set_xlim(-1, data_len)
        ax1_ticks = self.ax1.get_yticks()
        ax2_ticks = [int(i/max_ax1*max_ax2) for i in ax1_ticks]
        self.ax2.set_yticks(ax2_ticks)
        self.ax2.set_ylim(-0.5, max_ax2)
        legend1 = self.ax1.legend(loc=2, mode="expand",
            bbox_to_anchor=(0., 1.02, 1., .102))
        legend2 = self.ax2.legend(loc=1, mode="expand",
            bbox_to_anchor=(0.5, 1.02, 0.5, .102))
        setp(legend1.get_frame(), visible=False)
        setp(legend2.get_frame(), visible=False)
        setp(legend1.get_texts(), size=int(self.cp.get("Sizes", "LegendSize")))
        setp(legend2.get_texts(), size=int(self.cp.get("Sizes", "LegendSize")))

    def parse_data(self):
        pass

    def run(self, fp):
        self.file = fp
        self.parse_data()
        self.build_canvas()
        self.draw()
        self.write_graph()


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


def configure():
    usage = "usage: %prog [options] graph_out_filename data_out_filename"
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", help="PR Graph config file",
        dest="config")
    opts, args = parser.parse_args()

    if not args:
        parser.print_help()
        print "\nMust specify a output filename (or '-' for stdout)"
        sys.exit(1)

    filename = args[0]
    if filename != '-':
        fp = open(filename, 'w')
    else:
        fp = sys.stdout

    if len(args) < 2:
        parser.print_help()
        print "\nMust specify an output filename for the data files (or '-' for " \
            "stdout)"
        sys.exit(1)
    filename2 = args[1]
    if filename != '-':
        fp2 = open(filename2, 'w')
    else:
        fp2 = sys.stdout

    if not opts.config:
        parser.print_help()
        print "\nMust pass a config file."

    cp = ConfigParser.ConfigParser()
    cp.readfp(open(opts.config, "r"))
    return cp, fp, fp2

def main():
    cp, fp, fp2 = configure()

    # Generate the graphs
    pr = PRGraph(cp)
    ds = DataSource(cp)
    ds.run()
    pr.data1 = ds.query_jobs()
    pr.data2 = ds.query_users()
    pr.run(fp)

    # Generate the JSON
    ods = OIMDataSource(cp)
    prd = PRData(cp)
    prd.set_num_sites(len(ods.query_sites()))
    prd.set_num_jobs(sum(pr.data1)*60)
    prd.set_num_users(ds.query_user_num())
    ces, ses = ods.query_ce_se()
    prd.set_num_ces(ces)
    prd.set_num_ses(ses)
    prd.run(fp2)

if __name__ == '__main__':
    main()

