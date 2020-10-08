
import os
import sys
import signal
import logging
import optparse
import configparser
import time

from .common import log, get_files, commit_files
from .oim_datasource import OIMDataSource
from .gracc_datasource import HourlyJobsDataSource, DailyDataSource, \
    MonthlyDataSource
from .transfer_datasource import DataSourceTransfers
from .data import Data
from .display_graph import DisplayGraph

def configure():
    usage = "usage: %prog -c config_file"
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", help="PR Graph config file",
        dest="config", default="/etc/osg_display/osg_display.conf")
    parser.add_option("-q", "--quiet", help="Reduce verbosity of output",
        dest="quiet", default=False, action="store_true")
    parser.add_option("-d", "--debug", help="Turn on debug output",
        dest="debug", default=False, action="store_true")
    parser.add_option("-T", "--notimeout",
        help="Disable alarm timeout; useful for initial run",
        dest="notimeout", default=False, action="store_true")
    opts, args = parser.parse_args()

    if not opts.config:
        parser.print_help()
        print()
        log.error("Must pass a config file.")
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

    cp = configparser.SafeConfigParser()
    cp.readfp(open(opts.config, "r"))

    cp.notimeout = opts.notimeout

    logging.basicConfig(filename=cp.get("Settings", "logfile"))

    for handler in log.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - " \
            "%(message)s")
        handler.setFormatter(formatter)

    return cp

def main():
    watchB=time.time()
    cp = configure()

    # Set the alarm in case if we go over time
    if cp.notimeout:
        log.debug("Running script with no timeout.")
    else:
        timeout = int(cp.get("Settings", "timeout"))
        signal.alarm(timeout)
        log.debug("Setting script timeout to %i." % timeout)

    # Hourly graphs (24-hours)
    watchS=time.time()
    hjds = HourlyJobsDataSource(cp)
    hjds.run()
    dg = DisplayGraph(cp, "jobs_hourly")
    jobs_data, hours_data = hjds.query_jobs()
    dg.data = [i/1000 for i in jobs_data]
    num_jobs = sum(jobs_data)
    dg.run("jobs_hourly")
    hjds.disconnect()
    log.debug("Time log - Hourly Jobs Query Time: %s", (time.time() - watchS))
    watchS=time.time()
    dg = DisplayGraph(cp, "hours_hourly")
    dg.data = [float(i)/1000. for i in hours_data]
    dg.run("hours_hourly")
    log.debug("Time log - Hourly Jobs Graph Time: %s", (time.time() - watchS))
    # Generate the more-complex transfers graph

    watchS=time.time()
    dst = DataSourceTransfers(cp)
    dst.run()
    log.debug("Time log - Hourly Transfer Query Time: %s", (time.time() - watchS))
    watchS=time.time()
    dg = DisplayGraph(cp, "transfer_volume_hourly")
    dg.data = [i[1]/1024./1024. for i in dst.get_data()]
    log.debug("Transfer volumes: %s" % ", ".join([str(float(i)) for i in \
        dg.data]))
    dg.run("transfer_volume_hourly")
    transfer_data = dst.get_data()
    dg = DisplayGraph(cp, "transfers_hourly")
    dg.data = [int(i[0])/1000. for i in dst.get_data()]
    dg.run("transfers_hourly")
    num_transfers = sum([i[0] for i in transfer_data])
    transfer_volume_mb = sum([i[1] for i in transfer_data])
    dst.disconnect()
    log.debug("Time log - Hourly Transfer Graph Time: %s", (time.time() - watchS))

    # Daily (30-day graphs)
    watchS=time.time()
    dds = DailyDataSource(cp)
    dds.run()
    # Jobs graph
    jobs_data_daily, hours_data_daily = dds.query_jobs()
    dds.disconnect()
    log.debug("Time log - 30-Day Query Time: %s", (time.time() - watchS))
    # Job count graph
    watchS=time.time()
    dg = DisplayGraph(cp, "jobs_daily")
    dg.data = [float(i)/1000. for i in jobs_data_daily]
    num_jobs_hist = sum(jobs_data_daily)
    dg.run("jobs_daily", mode="daily")
    log.debug("Time log - 30-Day Count Graph Time: %s", (time.time() - watchS))
    # CPU Hours graph
    watchS=time.time()
    dg = DisplayGraph(cp, "hours_daily")
    dg.data = [float(i)/1000000. for i in hours_data_daily]
    num_hours_hist = sum(hours_data_daily)
    dg.run("hours_daily", mode="daily")
    log.debug("Time log - 30-Day CPU Graph Time: %s", (time.time() - watchS))
    # Transfers data
    watchS=time.time()
    transfer_data_daily, volume_data_daily = dds.query_transfers()
    log.debug("Time log - 30-Day Transfer Query Time: %s", (time.time() - watchS))
    # Transfer count graph
    watchS=time.time()
    dg = DisplayGraph(cp, "transfers_daily")
    dg.data = [float(i)/1000000. for i in transfer_data_daily]
    num_transfers_daily = sum(transfer_data_daily)
    dg.run("transfers_daily", mode="daily")
    log.debug("Time log - 30-Day Transfer Count Graph Time: %s", (time.time() - watchS))
    # Transfer volume graph
    watchS=time.time()
    dg = DisplayGraph(cp, "transfer_volume_daily")
    dg.data = [float(i)/1024.**3 for i in volume_data_daily]
    volume_transfers_hist = sum(volume_data_daily)
    dg.run("transfer_volume_daily", mode="daily")
    log.debug("Time log - 30-Day Transfer Volume Graph Time: %s", (time.time() - watchS))

    # Monthly graphs (12-months)
    watchS=time.time()
    mds = MonthlyDataSource(cp)
    mds.run()
    # Jobs graph
    jobs_data_monthly, hours_data_monthly = mds.query_jobs()
    mds.disconnect()
    log.debug("Time log - 12-Month Query Time: %s", (time.time() - watchS))
    # Job count graph
    watchS=time.time()
    dg = DisplayGraph(cp, "jobs_monthly")
    dg.data = [float(i)/1000000. for i in jobs_data_monthly]
    num_jobs_monthly = sum(jobs_data_monthly)
    dg.run("jobs_monthly", mode="monthly")
    log.debug("Time log - 12-Month Job Count Graph Time: %s", (time.time() - watchS))
    # Hours graph
    watchS=time.time()
    dg = DisplayGraph(cp, "hours_monthly")
    dg.data = [float(i)/1000000. for i in hours_data_monthly]
    num_hours_monthly = sum(hours_data_monthly)
    dg.run("hours_monthly", mode="monthly")
    log.debug("Time log - 12-Month Hour Graph Time: %s", (time.time() - watchS))
    # Transfers graph
    watchS=time.time()
    transfer_data_monthly, volume_data_monthly = mds.query_transfers()
    log.debug("Time log - 12-Month Transfer Query Time: %s", (time.time() - watchS))
    # Transfer count graph
    watchS=time.time()
    dg = DisplayGraph(cp, "transfers_monthly")
    dg.data = [float(i)/1000000. for i in transfer_data_monthly]
    num_transfers_monthly = sum(transfer_data_monthly)
    dg.run("transfers_monthly", mode="monthly")
    log.debug("Time log - 12-Month Transfer Count Graph Time: %s", (time.time() - watchS))
    # Transfer volume graph
    watchS=time.time()
    dg = DisplayGraph(cp, "transfer_volume_monthly")
    dg.data = [float(i)/1024.**3 for i in volume_data_monthly]
    volume_transfers_monthly = sum(volume_data_monthly)
    dg.run("transfer_volume_monthly", mode="monthly")
    log.debug("Time log - 12-Month Transfer Volume Graph Time: %s", (time.time() - watchS))
    # Pull OIM data
    watchS=time.time()
    ods = OIMDataSource(cp)
    num_sites = len(ods.query_sites())
    ces, ses = ods.query_ce_se()
    log.debug("Time log - OIM Time: %s", (time.time() - watchS))

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
    log.debug("Time log - Total Time: %s", (time.time() - watchB))

main_unwrapped = main

def main():

    try:
        main_unwrapped()
    except SystemExit:
        raise
    except (Exception, KeyboardInterrupt) as e:
        log.error(str(e))
        log.exception(e)
        raise

if __name__ == '__main__':
    main()

