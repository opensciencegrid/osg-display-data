
import os
import sys
import signal
import logging
import optparse
import ConfigParser

from common import log, get_files, commit_files
from oim_datasource import OIMDataSource
from gratia_datasource import HourlyJobsDataSource, DailyDataSource, \
    MonthlyDataSource
from transfer_datasource import DataSourceTransfers
from data import Data
from display_graph import DisplayGraph

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

    if not opts.quiet:
        log.info("Reading from log file %s." % opts.config)

    cp = ConfigParser.SafeConfigParser()
    cp.readfp(open(opts.config, "r"))

    logging.basicConfig(filename=cp.get("Settings", "logfile"))

    for handler in log.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - " \
            "%(message)s")
        handler.setFormatter(formatter)

    return cp

def main():
    cp = configure()

    # Set the alarm in case if we go over time
    timeout = int(cp.get("Settings", "timeout"))
    signal.alarm(timeout)
    log.debug("Setting script timeout to %i." % timeout)

    # Hourly graphs (24-hours)
    hjds = HourlyJobsDataSource(cp)
    hjds.run()
    dg = DisplayGraph(cp, "jobs_hourly")
    jobs_data, hours_data = hjds.query_jobs()
    dg.data = [i/1000 for i in jobs_data]
    num_jobs = sum(jobs_data)
    dg.run("jobs_hourly")
    hjds.disconnect()
    dg = DisplayGraph(cp, "hours_hourly")
    dg.data = [float(i)/1000. for i in hours_data]
    dg.run("hours_hourly")
    # Generate the more-complex transfers graph
    dst = DataSourceTransfers(cp)
    dst.run()
    dg = DisplayGraph(cp, "transfer_volume_hourly")
    dg.data = [i[1]/1024./1024. for i in dst.get_data()]
    log.debug("Transfer volumes: %s" % ", ".join([str(float(i)) for i in \
        dg.data]))
    dg.run("transfer_volume_hourly")
    transfer_data = dst.get_data()
    dg = DisplayGraph(cp, "transfers_hourly")
    dg.data = [i[0]/1000. for i in dst.get_data()]
    dg.run("transfers_hourly")
    num_transfers = sum([i[0] for i in transfer_data])
    transfer_volume_mb = sum([i[1] for i in transfer_data])
    dst.disconnect()

    # Daily (30-day graphs)
    dds = DailyDataSource(cp)
    dds.run()
    # Jobs graph
    jobs_data_daily, hours_data_daily = dds.query_jobs()
    dds.disconnect() 
    # Job count graph
    dg = DisplayGraph(cp, "jobs_daily")
    dg.data = [float(i)/1000. for i in jobs_data_daily]
    num_jobs_hist = sum(jobs_data_daily)
    dg.run("jobs_daily", mode="daily")
    # CPU Hours graph
    dg = DisplayGraph(cp, "hours_daily")
    dg.data = [float(i)/1000000. for i in hours_data_daily]
    num_hours_hist = sum(hours_data_daily) 
    dg.run("hours_daily", mode="daily")
    # Transfers data
    transfer_data_daily, volume_data_daily = dds.query_transfers()
    # Transfer count graph
    dg = DisplayGraph(cp, "transfers_daily")
    dg.data = [float(i)/1000000. for i in transfer_data_daily]
    num_transfers_daily = sum(transfer_data_daily)
    dg.run("transfers_daily", mode="daily")
    # Transfer volume graph 
    dg = DisplayGraph(cp, "transfer_volume_daily")
    dg.data = [float(i)/1024.**3 for i in volume_data_daily]
    volume_transfers_hist = sum(volume_data_daily)
    dg.run("transfer_volume_daily", mode="daily")

    # Monthly graphs (12-months)
    mds = MonthlyDataSource(cp)
    mds.run()
    # Jobs graph
    jobs_data_monthly, hours_data_monthly = mds.query_jobs()
    mds.disconnect()
    # Job count graph
    dg = DisplayGraph(cp, "jobs_monthly")
    dg.data = [float(i)/1000000. for i in jobs_data_monthly]
    num_jobs_monthly = sum(jobs_data_monthly)
    dg.run("jobs_monthly", mode="monthly")
    # Hours graph
    dg = DisplayGraph(cp, "hours_monthly")
    dg.data = [float(i)/1000000. for i in hours_data_monthly]
    num_hours_monthly = sum(hours_data_monthly)
    dg.run("hours_monthly", mode="monthly")
    # Transfers graph
    transfer_data_monthly, volume_data_monthly = mds.query_transfers()
    # Transfer count graph
    dg = DisplayGraph(cp, "transfers_monthly")
    dg.data = [float(i)/1000000. for i in transfer_data_monthly]
    num_transfers_monthly = sum(transfer_data_monthly)
    dg.run("transfers_monthly", mode="monthly")
    # Transfer volume graph
    dg = DisplayGraph(cp, "transfer_volume_monthly")
    dg.data = [float(i)/1024.**3 for i in volume_data_monthly]
    volume_transfers_monthly = sum(volume_data_monthly)
    dg.run("transfer_volume_monthly", mode="monthly")

    # Pull OIM data
    ods = OIMDataSource(cp)
    num_sites = len(ods.query_sites())
    ces, ses = ods.query_ce_se()

    # Generate the JSON
    log.debug("Starting JSON creation")
    d = Data(cp)
    d.add_datasource(mds)
    d.add_datasource(hjds)
    d.add_datasource(dst)
    d.add_datasource(dds)
    d.add_datasource(ods)
    # Monthly data
    log.debug("Done creating JSON.")

    name, tmpname = get_files(cp, "json")
    fd = open(tmpname, 'w')
    d.run(fd)
    commit_files(name, tmpname)

    log.info("OSG Display done!")

main_unwrapped = main

def main():

    try:
        main_unwrapped()
    except (SystemExit, Exception, KeyboardInterrupt), e:
        log.error(str(e))
        log.exception(e)
        raise

if __name__ == '__main__':
    main()

