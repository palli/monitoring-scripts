#!/usr/bin/env python
# This script will collect host names from "the sharepoint document"
# and update hostgroup of nagios hosts according to the "priority" column
# in the document

import pynag.Model
import sys

# filename = ''
# This is where we get our input from
filename = 'contactgroups.csv'

# hostgroups = {}
# This is a mapping of the priority name in our input
# and the actual name of the hostgroup
hostgroups = {}
hostgroups['A'] = 'priority-a'
hostgroups['B'] = 'priority-b'
hostgroups['C'] = 'priority-c'
hostgroups['D'] = 'priority-d'
hostgroups[''] = 'priority-undefined'

# hostgroup_members = {}
# A mapping that shows host names that belong to every hostgroup
hostgroup_members = {}


all_hosts = pynag.Model.Host.objects.filter(host_name__exists=True, register="1")
all_hostnames = map(lambda x: x.host_name, all_hosts)


if len(sys.argv) > 1:
    filename = sys.argv[1]

def main():
    hosts = parse_input_file(filename)
    update_hostgroup_members()
    put_host_in_correct_hostgroup(hosts)


def error(message):
    print "ERROR:", message
    sys.exit(1)


def warning(message):
    print "WARNING:", message


def info(message):
    print "INFO:", message


def debug(message):
    print "DEBUG:", message


def put_host_in_correct_hostgroup(hostlist):
    """ Iterate through every host and put it in correct hostgroup

        It will put hostlist[0].host_name in hostlist[0].hostgroup_name and
        remove the host from every other group in hostgroups.values()
    """
    info("Matching {length} hosts with current hostgroups".format(length=len(hostlist)))
    for host in hostlist:
        if host['host_name'] not in all_hostnames:
            warning("Host '{host_name}' in group {hostgroup_name} Not found. Skipping...'".format(**host))
            continue
        for hostgroup_name, members in hostgroup_members.items():
            # If host is supposed to be in this hostgroup, AND it is not already there
            if hostgroup_name == host['hostgroup_name'] and host['host_name'] not in members:
                info("Adding {host_name} to group {hostgroup_name}".format(**host))
                myhost = pynag.Model.Host.objects.get_by_shortname(host['host_name'])
                myhost.add_to_hostgroup(host['hostgroup_name'])
            # If host is not supposed to be here BUT he is here before a previous configuration
            elif hostgroup_name != host['hostgroup_name'] and host['host_name'] in members:
                info("Removing {host_name} from group {hostgroup_name}".format(**host))
                myhost = pynag.Model.Host.objects.get_by_shortname(host['host_name'])
                myhost.remove_from_hostgroup(host['hostgroup_name'])


def update_hostgroup_members():
    """ Update all hosts in hostgroups
    """
    for hostgroup_name in hostgroups.values():
        try:
            info("Fetching current members for hostgroup {hostgroup_name}".format(**locals()))
            hostgroup = pynag.Model.Hostgroup.objects.get_by_shortname(hostgroup_name)
            hosts = hostgroup.get_effective_hosts()
            host_names = map(lambda x: x.host_name, hosts)
            hostgroup_members[hostgroup_name] = host_names
        except KeyError:
            message = "hostgroup {hostgroup_name} not found. ".format(**locals())
            error(message)
    for k, v in hostgroup_members.items():
        debug("Hostgroup '{k}' currently has {length} members".format(k=k, length=len(v)))


def parse_input_file(filename):
    """ Read our input csv, and return a list in the form of:
     [
        {'host_name':X, 'hostgroup_name': X, ... },
     ]
    """
    info("Parsing input file {filename}".format(**locals()))
    result = []
    try:
        with open(filename) as f:
            for line_no, line in enumerate(f):
                columns = line.split(';')
                # Skip first line and invalid lines
                if line_no < 1 or len(columns) < 5:
                    continue
                host_name = columns[0]
                itogroup = columns[1]
                urelt = columns[2]
                priority = columns[3]
                if urelt != 'FALSE':
                    continue
                if not host_name:
                    continue
                if priority not in hostgroups:
                    error("Hostgroup '{priority}' not found in the 'hostgroups' dict. Please create it".format(**locals()))
                new_entry = {}
                new_entry['host_name'] = host_name
                new_entry['urel'] = host_name
                new_entry['hostgroup_name'] = hostgroups[priority]
                result.append(new_entry)
            return result
    except IOError, e:
        error("Cannot open file: {e}".format(**locals()))
if __name__ == '__main__':
    main()
