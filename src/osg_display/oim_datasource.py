
import urllib2

from xml.dom.minidom import parse

from common import log

"""
This module contains the OIMDataSource class, meant to be used to pull OIM data
from MyOSG
"""


class OIMDataSource(object):

    def __init__(self, cp):
        self.cp = cp

    resource_group_url = 'http://my.opensciencegrid.org/rgsummary/xml?datasource=' \
        'summary&all_resources=on&gridtype=on&gridtype_1=on&active=on&' \
        'active_value=1&disable=on&disable_value=0&' \
        'summary_attrs_showhierarchy=on&summary_attrs_showservice=on' \
        '&service=on&service_1=on&service_5=on&service_2=on&service_3=on'

    def query_sites(self):
        fd = urllib2.urlopen(self.resource_group_url)
        dom = parse(fd)
        sites = set()
        for site_dom in dom.getElementsByTagName("Site"):
            for name_dom in site_dom.getElementsByTagName("Name"):
                try:
                    sites.add(str(name_dom.firstChild.data))
                except:
                    pass
        log.debug("OIM returned the following sites: %s" % ", ".join(sites))
        log.info("OIM has %i registered sites." % len(sites))
        self.sites_results = sites
        return sites

    def query_ce_se(self):
        log.debug("Querying the following MyOSG URL: %s" % \
            self.resource_group_url)
        fd = urllib2.urlopen(self.resource_group_url)
        dom = parse(fd)
        ses = set()
        ces = set()
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
        self.ces_results, self.ses_results = ces, ses
        return len(ces), len(ses)

    def get_json(self):
        assert self.sites_results != None
        assert self.ces_results != None
        assert self.ses_results != None
        return {'num_ces': len(self.ces_results),
            'num_ses': len(self.ses_results),
            'num_sites': len(self.sites_results)}

