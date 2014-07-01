#!/usr/bin/env python
import argparse
import datetime as dt
import ConfigParser
import hashlib
import json
import os
import sys

from keystoneclient.v2_0 import client as keystone_client
from novaclient.v1_1 import client as nova_client
from novaclient.v1_1.security_groups import SecurityGroup

# Monkey-patch security-group code if it isn't in the client.
# This is needed for a1 and a2 which are running very old client libs.
if 'list_security_group' not in dir(nova_client.servers.ServerManager):
    def list_security_group(self, server):
        return self._list('/servers/%s/os-security-groups' % (server.id),
                          'security_groups', SecurityGroup)
    nova_client.servers.ServerManager.list_security_group = list_security_group

if 'list_security_group' not in dir(nova_client.servers.Server):
    def list_security_group(self):
        return self.manager.list_security_group(self)
    nova_client.servers.Server.list_security_group = list_security_group


def get_keystone_client(cfg):
    return keystone_client.Client(username=cfg.get('username'),
                                  password=cfg.get('password'),
                                  auth_url=cfg.get('auth_url'),
                                  insecure=True)


def get_nova_client(tenant, cfg):
    return nova_client.Client(cfg.get('username'), cfg.get('password'),
                              tenant, cfg.get('auth_url'), insecure=True,
                              service_type='compute',
                              endpoint_type="publicURL")


def get_nova_clients(keystone, cfg):
    clients = {}
    for tenant in keystone.tenants.list():
        clients[tenant.name] = get_nova_client(tenant.name, cfg)
    return clients


def secgroup_details(sec):
    return { 'id' : sec.id, 'name' : sec.name, 'rules' : sec.rules,
             'description' : sec.description, 'is_loaded' : sec.is_loaded(),
             'tenant_id' : sec.tenant_id, 'human_id' : sec.human_id }

def secgroup_str(sec):
    return json.dumps(secgroup_details(sec))

def secgroup_md5(sec):
    h = hashlib.new('md5')
    h.update(secgroup_str(sec))
    return h.hexdigest()


class SecgroupLog(object):
    def __init__(self, filename):
        self.filename = filename
        self._entries = {}
        self._try_load()


    def _try_load(self):
        if os.path.exists(self.filename):
            with open(self.filename, 'r') as fh:
                self._entries = json.load(fh)


    def secgroup_seen(self, secgroup):
        name = secgroup.name
        checksum = secgroup_md5(secgroup)
        if name not in self._entries:
            self._entries[name] = [checksum]
            return False
        elif checksum not in self._entries[name]:
            self._entries[name].append(checksum)
            return False
        else:
            return True


    def save(self):
        with open(self.filename, 'w') as fh:
            json.dump(self._entreis, fh)

def copy_security_group(secgroup, tenant):
    tenat_groups = [t for t in tenant.security_groups.list()]
    target_group = None
    print dir(secgroup)
    return
    if secgroup.name in [t.name for t in tenant_groups]:
        target_group = [t for t in tenant_groups if t.name == secgroup.name][0]
    else:
        target_group = tenant.security_groups.create(secgroup.name,
                                                     secgroup.description)
    for rule in secgroup.rules:
        pass
        #tenant.security_group_rules.create(pid, ip, f_port, t_port, cidr, sid)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, required=True,
                        help="Configuration file.")
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        print >>sys.stderr, "No file %s " % (args.config)
    
    config = ConfigParser.ConfigParser()
    config.read([args.config])
    secgroup_log = SecgroupLog(config.get('default', 'secgroup_log_file'))
    
    deployments = {}
    for deployment in config.sections():
        cfg = dict(config.items(deployment))
        keystone = get_keystone_client(cfg)
        deployments[deployment] =get_nova_clients(keystone, cfg)
    
    tenants = set(sum([ c.keys() for c in deployments.values()], []))
    for tenant in tenants:
        secgroups = deployments['essex'][tenant].security_groups.list()
        target_deployments = [k for k in deployments.keys() if k != 'essex']
        for name, clients in deployments.items():
            if name == 'essex':
                continue


if __name__ == '__main__':
    main()
