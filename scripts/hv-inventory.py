#!/usr/bin/env python
import json
import os
import re
import subprocess
import sys
"""
hv-inventory.py 
This script runs on hypervisors and queries libvirt and iscsiadm for
volume information and returns this in a JSON format.
"""


domain_rx = re.compile("instance-([0-9a-fA-F]{8})")
def _dom_to_id(domain):
    m = domain_rx.match(domain)
    if not m:
        print >> sys.stder, "Invalid domain name %s" % domain
    id = m.groups()
    return int("0x%s" % id, 16)

def get_virsh_domains():
    out = subprocess.check_output(['virsh', 'list'])
    rows = [l.split() for l in out.splitlines()]
    return [{'domain' : r[1], 'id' : _dom_to_id(r[1]), 'state' : r[2]}
            for r in rows if len(r) == 3 and r[0] != 'Id']

_iscsi_location_rx = re.compile(
".*ip-(\d+\.\d+\.\d+\.\d+):(\d+)-iscsi-iqn.2010-10.org.openstack:(volume-[0-9a-fA-F]{8})-lun-(\d+)")
def _parse_iscsi_location(loc):
    match = _iscsi_location_rx.match(loc)
    if not match:
        print 'ERROR matching rx for %s' % loc
        return
    ip, port, volume_id, lun = match.groups()
    return {'ip' : ip, 'port' : port, 'volume_id' : volume_id, 'lun' : lun}

_local_disk_rx = re.compile('.*/disk(\.local){0,1}$')
def get_virsh_volumes(domain):
    out = subprocess.check_output(['virsh', 'domblklist', domain])
    rows = [l.split() for l in out.splitlines()]
    output = []
    for row in rows:
        if len(row) != 2 or row[0] == 'Target' or _local_disk_rx.match(row[1]):
            continue
        dev_name, location = row
        volume = _parse_iscsi_location(location)
        volume['dev'] = dev_name
        output.append(volume)
    return output

_iscsi_session_rx = re.compile(
"(\w+): \[(\d+)\] (\d+\.\d+\.\d+\.\d+):(\d+),(\d+) iqn.2010-10.org.openstack:(volume-[0-9a-fA-F]{8})")
def get_iscsiadm_data():
    p = subprocess.Popen(['iscsiadm', '-m', 'session'],
                         stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    iscsi_data = []
    for line in p.stdout:
        if line == 'iscsiadm: No active sessions.':
            continue
        m = _iscsi_session_rx.match(line)
        if not m:
            print >> sys.stderr, "Error processing line: %s" % line
        mode, id, ip, port, lun, volume_id = m.groups()
        iscsi_data.append({'ip' : ip, 'port' : port, 'mode' : mode,
                           'lun' : lun, 'volume_id' : volume_id})
    return iscsi_data

def main():
    virsh_data = get_virsh_domains()
    for row in virsh_data:
        row['volumes'] = get_virsh_volumes(row['domain'])
    iscsiadm_data = get_iscsiadm_data()
    print json.dumps({'virsh' : virsh_data, 'iscsiadm' : iscsiadm_data})

if __name__ == '__main__':
    main()
