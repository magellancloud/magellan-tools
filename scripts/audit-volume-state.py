#!/usr/bin/env python
import argparse
import collections
import itertools
import json
import os
import re
import subprocess
import sys

HV_INVENTORY_SCRIPT = '/home/devoid/projects/hv-inventory.py'
# Run pdsh or pdcp, report errors and return stdout
def _pdsh(args, hosts, cmd='pdsh'):
    pdsh_cmd = [cmd, '-u', '15', '-w', ','.join(hosts)]
    pdsh_cmd.extend(args)
    print >> sys.stderr, '%s: %s' % (cmd, ' '.join(args))
    p = subprocess.Popen(pdsh_cmd, stdout=subprocess.PIPE, stderr=None)
    return p.stdout

# Process stdout from pdsh assuming that each script returned valid json.
# Fold that under the hostname that returned it.
_pdsh_line_rx = re.compile("^([^:]+): (.*)$")
def dshback_json(fh):
    hosts = collections.defaultdict(list)
    for line in fh:
        m = _pdsh_line_rx.match(line)
        if not m:
            print >> sys.stderr, "Error reading line: %s" % line
        host, part = m.groups()
        hosts[host].append(part)
    for host, parts in hosts.iteritems():
        hosts[host] = json.loads("".join(parts))
    return dict(hosts)

# Get data from hypervisor (iscsiadm and virsh)
def get_hv_data(hosts):
    if os.path.exists(HV_INVENTORY_SCRIPT):
        _pdsh([HV_INVENTORY_SCRIPT, '/tmp/hv-inventory.py'], hosts, cmd='pdcp')
    out = _pdsh(['python', '/tmp/hv-inventory.py'], hosts)
    data = dshback_json(out)
    return data

# Get instance data from DB
def _db_instance_data():
    cmd = ['ssh', 'm3-p', './inventory_all_instances']
    output = subprocess.check_output(cmd)
    rows  = [l.split() for l in output.splitlines()]
    data = []
    _state_map = {'active' : 'running', 'shutoff' : 'shutoff',
                  'error' : 'error', 'paused' : 'paused',
                  'suspended' : 'suspended'}
    for row in rows:
        domain = "instance-%08x" % int(row[0])
        data.append({'uuid' : row[1], 'domain' : domain, 'host' : row[2],
                     'id' : row[0], 'state' : _state_map.get(row[4], row[4])})
    return data

_volume_id_rx = re.compile("volume-([0-9a-fA-F]{8})")
def _volume_id_to_id(v):
    m = _volume_id_rx.match(v)
    id = m.groups()
    return int("0x%s" % id, 16)

# Get volume data from db
_volume_provider_rx = re.compile(
"(\d+\.\d+\.\d+\.\d+):(\d+),(\d+) iqn.2010-10.org.openstack:(volume-[0-9a-fA-F]{8})")
def _db_volume_data():
    cmd = ['ssh', 'm3-p', './inventory_volumes3']
    output = subprocess.check_output(cmd)
    rows = [l.split(';') for l in output.splitlines()]
    columns = ['id', 'instance_uuid', 'dev', 'user', 'project', 'iqn', 'status']
    volume_data = []
    for row in rows:
        data = dict(zip(columns, row))
        ip, port, lun = (None, None, None)
        volume_id = "volume-%08x" % int(data['id'])
        if data['iqn'] != 'None':
            m = _volume_provider_rx.match(data['iqn'])
            if not m:
                print >> sys.stderr, "Error processing row: %s" % (" ".join(row))
                continue
            ip, port, lun, volume_id = m.groups()
        final = {'id' : data['id'], 'instance_uuid' : data['instance_uuid'],
                 'dev' : data['dev'], 'ip' : ip, 'port' : port, 'lun' : lun,
                 'volume_id' : volume_id, 'status' : data['status']}
        volume_data.append(final)
    return volume_data

def _join_db_data(instance_data, volume_data):
    volumes = []
    for volume in volume_data:
        instance = {}
        uuid = volume.get('instance_uuid')
        print volume
        if uuid != 'None':
            instances = [i for i in instance_data if i['uuid'] == uuid]
            if len(instances) == 1:
                instance = instances[0]
            elif len(instances) > 1:
                print >> sys.stderr, "Multiple instances for volume %s" % (
                    volume['volume_id'])
            else:
                print "Unable to find instance for %s" % uuid
        volumes.append({'volume_id': volume['volume_id'],
                         'id': volume['id'], 'ip': volume['ip'],
                         'port': volume['port'], 'lun': volume['lun'],
                         'dev': volume['dev'],
                         'status' : volume['status'],
                         'instance_uuid': instance.get('uuid', None),
                         'instance_id': instance.get('id', None),
                         'host': instance.get('host', None)})
    print volumes[0]
    return volumes

def get_db_data(hosts):
    return _join_db_data(_db_instance_data(), _db_volume_data())

def _balance_matrix(m):
    n = collections.defaultdict(dict)
    for k1, v1 in m.iteritems():
        for k2, v2 in v1.iteritems():
            n[k2][k1] = v2
            n[k1][k2] = v2
    return dict(n)

#   v x         x           x    x       x  
#  i  x         x   x    x  x
# d   x         x        x  x    x
# div volume_id id  mode ip host vm-uuid dev
def volume_vote(entry, entry_type, votes):
    # These are the keys that each type cares about
    comparisons = {'t' : ['volume_id'],
                   'd' : ['volume_id', 'ip', 'host', 'instance_id'],
                   'i' : ['volume_id', 'ip', 'host'],
                   'v' : ['volume_id', 'host', 'instance_id']}  
    # Now define them in terms of the intersection of two types
    compare_matrix = {'d': {'i': ['volume_id', 'ip', 'host'],
                            'v': ['volume_id', 'host', 'instance_id'],
                            't': ['volume_id']},
                      'i': {'v': ['volume_id', 'host'],
                            't': ['volume_id']},
                      'v': {'t': ['volume_id']}}
    compare_matrix = _balance_matrix(compare_matrix)
    # Also include additional data when we match a vote or create a new one
    add_data = {'d' : ['instance_uuid', 'id', 'status'],
                'i' : ['mode'],
                'v' : ['dev'],
                't' : ['sessions']}
    my_comparisons = compare_matrix[entry_type]
    voted = False

    for vote in votes:
        # candidate must be true and reject must be false
        # candidate - vote must have an other_entry_type checked
        # reject    - must not fail on other_entry_type keys we care about
        candidate, reject = (False, False)
        for other_entry_type, entry_comparisons in my_comparisons.iteritems():
            if reject == True:
                break
            if vote[other_entry_type] != '-':
                candidate = True
                for key in entry_comparisons:
                    if key not in vote or vote.get(key) != entry.get(key):
                        reject = True
                        break
        if candidate and not reject:
            vote[entry_type] = entry_type
            for key in add_data[entry_type]:
                vote[key] = entry[key]
            voted = True
            break
    if not voted:
        # ok, we failed to find a match, make a new vote
        vote = { 'd': '-', 'i': '-', 'v' : '-', 't' : '-' }
        vote[entry_type] = entry_type
        for key in comparisons[entry_type]:
            vote[key] = entry[key]
        for key in add_data[entry_type]:
            vote[key] = entry[key]
        votes.append(vote)
    
def print_results(votes):
    def _dvi(row):
        return '%s%s%s%s' % (row['t'], row['d'], row['i'], row['v'])
    columns = {'TDIV' : _dvi, 'volume_id': None, 'id': None, 'mode': None,
               'ip': None, 'host': None, 'instance_uuid': None, 'dev': None,
               'sessions' : None, 'status' : None}
    sorted_columns = [('TDIV', 3), 
                      ('volume_id', 15),
                      ('id', 6),
                      ('mode', 4),
                      ('status', 14),
                      ('ip', 15),
                      ('host', 7),
                      ('instance_uuid', 36),
                      ('dev', 10),
                      ('sessions', 8)]
    formatstr = " ".join(map(lambda c: "%%%ds" % c[1], sorted_columns))
    sorted_columns = map(lambda c: c[0], sorted_columns)
    print formatstr % tuple(sorted_columns)
    for data in votes:
        print formatstr % tuple([columns[c](data) if columns[c] is not None
                                 else data.get(c, '-') for c in sorted_columns])

# Hack to process hosts like pdsh
def _evaluate_host_str(host_str):
    return subprocess.check_output(['/home/desai/mht', '-w', host_str,
                                    'echo', '%s']).splitlines()

_target_name_rx = re.compile(
    "iqn.2010-10.org.openstack:(volume-[0-9a-fA-F]{8})")
def get_itadm_data(hosts):
    out = _pdsh(['itadm', 'list-target'], hosts=hosts)
    data = []
    for line in out:
        row = line.split()
        if len(row) != 4:
            continue
        m = _target_name_rx.match(row[1])
        if not m:
            print >> sys.stderr, "Unable to match iqn string %s" % row[2]
            continue
        volume_id = m.group(1) 
        data.append({'volume_id' : volume_id, 'status' : row[2],
                     'sessions' : row[3]})
    return data
        
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--clear-cache', action='store_true',
                        help='Clear the cache before running.')
    parser.add_argument('-n', '--no-cache', action='store_true',
                        help='Do not populate the cache on this run.')
    parser.add_argument('-d', '--cache-dir', type=str,
                        help='Directory for cache data.')
    parser.add_argument('-H', '--hosts', type=str,
                        help='Hosts to gather this info on.') 
    options = parser.parse_args()
    cache_dir = '/tmp/magellan-volume-state-audit'
    cache_files = {'db.json': None, 'hv.json': None, 'hv.hosts.json': None,
                   'it.json': None}
    hosts = 'cc[1-504]-p'
    if options.hosts:
        hosts = options.hosts
    if options.cache_dir:
        cache_dir = options.cache_dir
    for f in cache_files.keys():
        cache_files[f] = os.path.join(cache_dir, f)
    
    # Remove files only
    if options.clear_cache:
        for filename in cache_files.values():
            if os.path.exists(filename):
                os.remove(filename)
    
    # mht has issues with multiple patterns comma delimited
    hosts = list(itertools.chain(*map(_evaluate_host_str, hosts.split(','))))
    db_data = None
    hv_data = None
    it_data = None
    # Now try to get data from cache
    if not options.no_cache:
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)
        missing = {}
        redo_all = False
        # Fail if files are missing
        for key, filename in cache_files.iteritems():
            if not os.path.exists(filename):
                missing[key] = True
        # Fail if we don't have the right hosts
        if 'hv.hosts.json' not in missing:
            with open(cache_files['hv.hosts.json'], 'r') as fh:
                cached_hosts = json.load(fh)
                for host in hosts:
                    if host not in cached_hosts:
                        redo_all = True 
                        break
        # Ok, load from cached data
        if not redo_all and 'hv.json' not in missing:
            with open(cache_files['hv.json'], 'r') as fh:
                hv_data = json.load(fh)
        if not redo_all and 'db.json' not in missing:
            with open(cache_files['db.json'], 'r') as fh:
                db_data = json.load(fh)
        if not redo_all and 'it.json' not in missing:
            with open(cache_files['it.json'], 'r') as fh:
                it_data = json.load(fh)
    # If we didn't load the data, make it now, and mabye cache it
    if hv_data is None:
        hv_data = get_hv_data(hosts)
        if not options.no_cache:
            with open(os.path.join(cache_dir, 'hv.json'), 'w') as fh:
                json.dump(hv_data, fh)
    if db_data is None:
        db_data = get_db_data(hosts)
        if not options.no_cache:
            with open(os.path.join(cache_dir, 'db.json'), 'w') as fh:
                json.dump(db_data, fh)
    if it_data is None:
        it_data = get_itadm_data(['v[1,3]-p'])
        if not options.no_cache:
            with open(os.path.join(cache_dir, 'it.json'), 'w') as fh:
                json.dump(it_data, fh)
    if not options.no_cache:
        with open(os.path.join(cache_dir, 'hv.hosts.json'), 'w') as fh:
            json.dump(hosts, fh)
    
    # Now start processing the data
    votes = []
    for volume in db_data:
        volume_vote(volume, 'd', votes)
    for host, hv_host_data in hv_data.iteritems():
        iscsi_sessions = hv_host_data['iscsiadm']
        for s in hv_host_data['iscsiadm']:
            entry = {'host': host, 'volume_id': s['volume_id'], 'ip': s['ip'],
                     'port': s['port'], 'mode': s['mode'], 'lun': s['lun']}
            volume_vote(entry, 'i', votes)
        domains = hv_host_data['virsh'] 
        for domain in domains:
            for v in domain['volumes']:
                entry = {'host': host, 'volume_id': v['volume_id'],
                         'id': _volume_id_to_id(v['volume_id']),
                         'instance_id': str(domain['id']), 'dev': v['dev'],
                         'domain': domain['domain']}
                volume_vote(entry, 'v', votes)
    for entry in it_data:
        volume_vote(entry, 't', votes)
    
    print_results(votes)     
                
if __name__ == '__main__':
    main()
