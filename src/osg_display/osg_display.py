
import os
import sys
import signal
import logging
import optparse
import ConfigParser

from common import log, get_files, commit_files
from oim_datasource import OIMDataSource
from gratia_datasource import DataSource, HistoricalDataSource
from transfer_datasource import DataSourceTransfers
from data import Data
from display_graph import DisplayGraph

_euid = os.geteuid()

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

    # Generate the graphs
    ds = DataSource(cp)
    ds.run()
    pr = DisplayGraph(cp, 1)
    jobs_data, hours_data = ds.query_jobs()
    pr.data = [i/1000 for i in jobs_data]
    num_jobs = sum(jobs_data)
    pr.run("jobs")
    user_num = ds.query_user_num()
    ds.disconnect()

    #pr = DisplayGraph(cp, 2)
    #pr.data = ds.query_users()
    #pr.run("users")

    # Historical (12-month graphs)
    ds = HistoricalDataSource(cp)
    ds.run()
    # Jobs graph
    jobs_data_hist, hours_data_hist = ds.query_jobs()
    ds.disconnect()
    # Job count graph
    pr = DisplayGraph(cp, 4)
    pr.data = [float(i)/1000000. for i in jobs_data_hist]
    num_jobs_hist = sum(jobs_data_hist)
    pr.run("jobs_hist", mode="historical")
    # Hours graph
    pr = DisplayGraph(cp, 5)
    pr.data = [float(i)/1000000. for i in hours_data_hist]
    num_hours_hist = sum(hours_data_hist)
    pr.run("hours_hist", mode="historical")
    # Transfers graph
    transfer_data_hist, volume_data_hist = ds.query_transfers()
    # Transfer count graph
    pr = DisplayGraph(cp, 6)
    pr.data = [float(i)/1000000. for i in transfer_data_hist]
    num_transfers_hist = sum(transfer_data_hist)
    pr.run("transfer_hist", mode="historical")
    # Transfer volume graph
    pr = DisplayGraph(cp, 7)
    pr.data = [float(i)/1024.**3 for i in volume_data_hist]
    volume_transfers_hist = sum(volume_data_hist)
    pr.run("transfer_volume_hist", mode="historical")

    # Generate the more-complex transfers graph
    dst = DataSourceTransfers(cp)
    dst.run()
    pr = DisplayGraph(cp, 3)
    pr.data = [i[1]/1024./1024. for i in dst.get_data()]
    log.debug("Transfer volumes: %s" % ", ".join([str(float(i)) for i in pr.data]))
    pr.run("transfers")
    transfer_data = dst.get_data()
    num_transfers = sum([i[0] for i in transfer_data])
    transfer_volume_mb = sum([i[1] for i in transfer_data])
    dst.disconnect()

    # Generate the JSON
    log.debug("Starting JSON creation")
    ods = OIMDataSource(cp)
    prd = Data(cp)
    prd.set_num_sites(len(ods.query_sites()))
    prd.set_num_jobs(num_jobs)
    prd.set_num_users(user_num)
    ces, ses = ods.query_ce_se()
    prd.set_total_hours(sum(hours_data))
    prd.set_num_ces(ces)
    prd.set_num_ses(ses)
    prd.set_num_transfers(num_transfers)
    prd.set_transfer_volume_mb(transfer_volume_mb)
    prd.set_transfer_volume_rate([i for i in dst.get_volume_rates()][-1])
    prd.set_jobs_rate(jobs_data[-1])
    prd.set_transfer_rate([i for i in dst.get_rates()][-1])
    # Historical data
    prd.set_num_transfers_hist(num_transfers_hist)
    prd.set_transfer_volume_mb_hist(volume_transfers_hist)
    prd.set_num_jobs_hist(num_jobs_hist)
    prd.set_total_hours_hist(num_hours_hist)
    log.debug("Done creating JSON.")

    name, tmpname = get_files(cp, "json")
    fd = open(tmpname, 'w')
    prd.run(fd)
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

