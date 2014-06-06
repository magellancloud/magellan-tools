#!/usr/bin/env python
import argparse
import ConfigParser
import datetime
import logging
import logging.config
import os
import sys
from keystoneclient.v2_0.client import Client as KeystoneClient
from glanceclient.v1.client import Client as GlanceV1Client
from glanceclient import exc as glance_exceptions

LOG = logging.getLogger('default')

def _download_image(deployment, image, filename):
    size = 0
    start = datetime.datetime.now()
    with open(filename, 'w') as fh:
        itr = deployment.glance.images.data(image.id)
        for blk in itr:
            size += len(blk)
            fh.write(blk)
    stop = datetime.datetime.now()
    delta = stop - start
    mb = 2 ** 20
    mbps = "%0.2f Mbps" % (size/mb/delta.total_seconds())
    LOG.debug('Downloaded image %s to %s (%s)' % (image.id, filename, mbps))


def _upload_image(deployment, image, filename, owner_map=None):
    wanted_params = ('name', 'disk_format', 'container_format',
                     'min_disk', 'min_ram', 'owner', 'size', 'is_public',
                     'protected', 'id')
    kwargs = dict([(p, getattr(image, p)) for p in wanted_params])
    if owner_map is not None:
        kwargs['owner'] = owner_map[kwargs['owner']]
    with open(filename, 'r') as fh:
        kwargs['data'] = fh
	start = datetime.datetime.now()
        new_image = deployment.glance.images.create(**kwargs)
	delta = datetime.datetime.now() - start
	size = new_image.size
    	mb = 2 ** 20
	mbps = "%0.2f Mbps" % (size/mb/delta.total_seconds())
        LOG.debug('Uploaded image %s to %s (%s)' %
                 (image.id, deployment.name, mbps))
    os.unlink(filename)


def _sync_metadata(image, source, destination, owner_map=None):
    for member in source.glance.image_members.list(image=image.id):
        destination_image_members = destination.glance.image_members
        member_id = member.member_id
        can_share = member.can_share
        if owner_map is not None:
            member_id = owner_map[member_id]
        d_member = None
        try:
            # Hack because members.get() does not work.
            # See https://bugs.launchpad.net/glance/+bug/1326955
            f = lambda m: True if m.member_id == member_id else False
            d_members = filter(f, destination_image_members.list(member_id))
            if len(d_members) != 0:
                d_member = d_members[0]
        except glance_exceptions.HTTPNotFound:
            pass
        if d_member and d_member.can_share == member.can_share:
            LOG.debug("Member %s correct for image %s in %s" %
                      (member_id, image.id, destination.name))
        elif d_member and d_member.can_share != member.can_share:
            LOG.debug("Member %s setting can_share to %s for image %s in %s" %
                      (member_id, image.id, member.can_share,
                       destination.name)) 
            destination_image_members.delete(image.id, member_id)
            destination_image_members.create(image.id, member_id, can_share)
        else:
            LOG.debug("Create member %s for image %s in %s" %
                      (member_id, image.id, destination.name))
            destination_image_members.create(image.id, member_id, can_share)


def do_sync(source, destination, image_cache, owner_map=None, pool=None):
    for image in source.glance.images.list(is_public=None):
        if image.deleted:
            LOG.debug('Skipping image %s, is deleted.' % (image.id))
            continue
        if image.status != 'active':
            LOG.debug('Skipping image %s, is not active.' % (image.id))
            continue
        dest_image = None
        try:
            dest_image = destination.glance.images.get(image.id)
        except glance_exceptions.HTTPNotFound:
            pass
        if dest_image:
            LOG.debug('Image %s already exists in %s' %
                      (image.id, destination.name))
        else:
            tempfile = os.path.join(image_cache, image.id)
            _download_image(source, image, tempfile)
            _upload_image(destination, image, tempfile, owner_map=owner_map)
        _sync_metadata(image, source, destination, owner_map=owner_map)
        LOG.info('Image %s sync from %s to %s' %
                 (image.id, source.name, destination.name))


class OwnerMap(object):
    # TODO(devoid): Need to make this multiple-deployment capable
    def __init__(self, source, destination):
        self.s2d = {}
        self.d2s = {}
        src_k = source.keystone
        dst_k = destination.keystone
        src_users = dict([(u.name, u) for u in src_k.users.list()])
        src_tenants = dict([(t.name, t) for t in src_k.tenants.list()])
        dst_users = dict([(u.name, u) for u in dst_k.users.list()])
        dst_tenants = dict([(t.name, t) for t in dst_k.tenants.list()])
        src_owners = dict(src_users.items() + src_tenants.items())
        dst_owners = dict(dst_users.items() + dst_tenants.items())
        for name, owner in src_owners.iteritems():
            if name in dst_owners:
                dst_id = dst_owners[name].id
                self.s2d[owner.id] = dst_id
                self.d2s[dst_id] = owner.id
        for name, owner in dst_owners.iteritems():
            if owner.id in self.d2s:
                continue
            LOG.warn('User %s in %s not found in %s' %
                     (name, destination.name, source.name))
    
    def __getitem__(self, id):
        if id in self.s2d:
            return self.s2d[id]
        elif id in self.d2s:
            return self.d2s[id]
        else:
            raise KeyError("Unknown user %s" % id)

class Deployment(object):
    def __init__(self, config, section):
        kwargs = dict([(o, config.get(section, o))
                       for o in config.options(section)])
        self.name = section
        self.keystone = KeystoneClient(**kwargs)
        glance_endpoint = self._get_endpoint('image')
        self.glance = GlanceV1Client(glance_endpoint['internalURL'],
                                     token=self.keystone.auth_token)

    def _get_endpoint(self, endpoint_type):
        e = self.keystone.service_catalog.get_endpoints()
        if endpoint_type not in e:
            return None
        return e[endpoint_type][0]


def _get_config(filename):
    filename = os.path.abspath(filename)
    if not os.path.exists(filename):
        print "Config file %s does not exist!" % filename
        sys.exit(1)
    config = ConfigParser.ConfigParser()
    config.read(filename)
    return config


def _setup_image_cache(path):
    path = os.path.abspath(path)
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def _setup_logging(logging_config_file):
    if logging_config_file:
        logging.config.fileConfig(logging_config_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-c', type=str, required=True,
                        help='Config file for keystone logins.')
    parser.add_argument('--logfile', type=str,
                        help='Path to log file to use. Otherwise STDERR used.')
    parser.add_argument('--image-cache-dir', '-i', type=str, required=True,
                        help='Directory to cache images in before upload.')
    parser.add_argument('source', type=str, help='Where to copy images from.')
    parser.add_argument('destination',
                        type=str, help='Where to copy images to.')
    args = parser.parse_args()
    config = _get_config(args.config)
    _setup_logging(args.config)
    image_cache = _setup_image_cache(args.image_cache_dir)
    source = Deployment(config, args.source)
    destination = Deployment(config, args.destination)
    owner_map = OwnerMap(source, destination)
    do_sync(source, destination, image_cache, owner_map=owner_map)

if __name__ == '__main__':
    main()
