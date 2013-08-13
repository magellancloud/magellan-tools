#!/usr/bin/env python
import argparse
import os
import re
import string
import subprocess
import sys
import tempfile

from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
import smtplib

from keystoneclient.v2_0 import client as keystone

MIGRATE_SNAPSHOT_PREFIX = "migrate"

def _add_arg(f, *args, **kwargs):
    """Bind CLI arguments to a shell.py `do_foo` function."""
    if not hasattr(f, 'arguments'):
        f.arguments = []
    if (args, kwargs) not in f.arguments:
        f.arguments.insert(0, (args, kwargs))

def arg(*args, **kwargs):
    """Decorator for CLI arguments."""
    def _decorator(func):
        _add_arg(func, *args, **kwargs)
        return func
    return _decorator

def get_text_from_editor(template):
    """Enter an editor to gather text. Strip out comment lines."""
    def which(cmd):
        return subprocess.check_output(
            ' '.join(['which', cmd]), shell=True).rstrip()
    editor = os.environ.get('EDITOR', which('vi'))
    with tempfile.NamedTemporaryFile(delete=False) as fh:
        fh.write(template)
        filename = fh.name
    os.system(" ".join([editor, filename]))
    text = []
    comment = re.compile("^#")
    with open(filename, 'r') as fh:
        for line in fh:
            if not comment.match(line):
                text.append(line)
    os.unlink(filename)
    return "\n".join(text)

def send_mail(send_from, send_to, subject, text, files=[], server="localhost"):
    assert type(send_to)==list
    assert type(files)==list
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach( MIMEText(text) )
    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

class Shell(object):
    
    def get_base_parser(self):
        desc = self.__doc__ or ''
        parser = argparse.ArgumentParser(description=desc.strip())
        return parser
    
    def get_subcommand_parser(self):
        parser = self.get_base_parser()
        self.subcommands = {}
        subparsers = parser.add_subparsers(metavar='<subcommand>')
        self._find_actions(subparsers)
        return parser

    def _find_actions(self, subparsers):
        for attr in (a for a in dir(self.__class__) if a.startswith('do_')):
            command = attr[3:].replace('_', '-')
            callback = getattr(self.__class__, attr)
            desc = callback.__doc__ or ''
            action_help = desc.strip().split('\n')[0]
            arguments = getattr(callback, 'arguments', [])
            subparser = subparsers.add_parser(command,
                help=action_help,
                description=desc,
                add_help=False,
            )
            subparser.add_argument('-h', '--help',
                action='help',
                help=argparse.SUPPRESS,
            )
            self.subcommands[command] = subparser
            for (args, kwargs) in arguments:
                subparser.add_argument(*args, **kwargs)
            subparser.set_defaults(func=callback)

    def main(self, argv):
        parser = self.get_subcommand_parser()
        args = parser.parse_args(argv)
        args.func(self, args)

def _tbl_text(text, delimiter="\t"):
    rows = text.split("\n")
    headers = rows[0].split(delimiter)
    final = []
    for row in rows[1:]:
        cols = row.split(delimiter)
        if len(headers) != len(cols):
            continue
        final.append(dict(zip(headers, cols)))
    return final

def get_keystone_client(tenant=None):
    def _get_os_env(tenant=None):
        try:
            if not tenant:
                tenant = os.environ['OS_TENANT_NAME']
            username = os.environ['OS_USERNAME']
            password = os.environ['OS_PASSWORD']
            auth_url = os.environ['OS_AUTH_URL']
            return (username, password, tenant, auth_url)
        except KeyError, e:
            print >>sys.stderr, (
                "Environment variable %s not defined!\n"
                "Have you sourced your OpenStack .novarc file?\n"
                % (e))
            sys.exit(1)
    (username, password, tenant, auth_url) = _get_os_env(tenant=tenant)
    return keystone.Client(
        username=username, password=password,
        tenant_name=tenant, auth_url=auth_url)

class VolumeServerContainer(object):
    """Need this container for VolumeServer objects for get_server behavior."""
    def __init__(self, servers=[]):
        self.servers = servers
    
    def get_server(self, hostname):
        """Get the server based on hostname, service_host or migrate_iface."""
        for server in self.servers:
            if server.host == hostname:
                return server
            elif server.service_host == hostname:
                return server
            elif server.migrate_iface == hostname:
                return server
        Exception("Unknown server %s." % hostname)
    
    def all(self):
        return self.servers

class VolumeServer(object):
    def __init__(self, host, pool='dpool/', service_host=None,
                 migrate_iface=None):
        self.host = host
        self.pool = pool
        self.service_host = service_host if service_host else host
        self.migrate_iface = migrate_iface if migrate_iface else host

    def volume_iqn(self, volume):
        ip = self._get_ip_address(self.migrate_iface)
        volume_name = "volume-%08x" % (volume.id)
        return "%s:3260,1 iqn.2010-10.org.openstack:%s" % (ip, volume_name)

    def _get_ip_address(self, hostname):
        p1 = subprocess.Popen(['cat', '/etc/hosts'], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(['grep', hostname], stdin=p1.stdout,
                              stdout=subprocess.PIPE)
        p1.stdout.close()
        (ip, host) = p2.communicate()[0].split()
        return ip 

    def volume_pool_name(self, volume):
        vol_name = "volume-%08x" % (volume.id)
        return "%s%s" % (self.pool, vol_name) 

    def volume_migrate_snapshots(self, volume):
        """Return list of dictionaries containing volume info."""
        cmd = ['ssh', self.host, 'zfs', 'list', '-t', 'snapshot']
        vols = _tbl_text(subprocess.check_output(cmd), delimiter=None)
        vol_name = self.volume_pool_name(volume)
        vol_regex = re.compile("^%s@%s" % (vol_name, MIGRATE_SNAPSHOT_PREFIX))
        return [ v for v in vols if vol_regex.match(v.get('NAME', "")) ]
    
    def max_snapshot_number(self, volume):
        """Note that our snapshots start at 1, so 0 implies no snapshots"""
        snapshots = self.volume_migrate_snapshots(volume) 
        max_count = 0
        for snap in snapshots:
            snap = snap['NAME']
            parts = snap.split('_')
            count = parts[len(parts)-1]
            max_count = max(int(count), max_count)
        return max_count

    def _volume_snapshot_name(self, volume, count):
        vol_name = self.volume_pool_name(volume)
        return "%s@%s_%d" % (vol_name, MIGRATE_SNAPSHOT_PREFIX, count)

    def max_snapshot_name(self, volume):
        count = self.max_snapshot_number(volume)
        return self._volume_snapshot_name(volume, count)

    def _unique_volume_snapshot_name(self, volume):
        max_count = self.max_snapshot_number(volume)
        return self._volume_snapshot_name(volume, max_count + 1)

    def snapshot(self, volume):
        snapshot_name = self._unique_volume_snapshot_name(volume)
        cmd = ['ssh', self.host, 'zfs', 'snapshot', snapshot_name]
        print " ".join(cmd)
        subprocess.check_output(cmd)
        return snapshot_name

    def _send_snapshot(self, dest, volume, snapshot_name, increment=None):
        """Command line logic for sending snapshots."""
        cmd = ['ssh', self.host, 'zfs', 'send' ]
        if increment:
            cmd.extend(['-i', increment])
        dest_volume_name = dest.volume_pool_name(volume)
        cmd.extend([snapshot_name, "|", 'ssh', dest.migrate_iface, 'zfs',
                    'recv', dest_volume_name])
        print " ".join(cmd)
        subprocess.check_call(cmd)

    def send_snapshot(self, dest, volume, snapshot_name):
        """Send snapshot to dest, trying to do incremental transfer."""
        ours = self.max_snapshot_number(volume)
        theirs = dest.max_snapshot_number(volume)
        inc = min(ours, theirs)
        # Zero implies no snapshots on one side or the other
        if inc != 0:
            self._send_snapshot(dest, volume, snapshot_name, increment=inc)
        self._send_snapshot(dest, volume, snapshot_name)

    def destroy(self, pool_spec):
        """Issue a destroy against a poolname."""
        cmd = ['ssh', self.host, 'zfs', 'destroy', pool_spec]
        print " ".join(cmd)
        subprocess.check_call(cmd)

    # Functions from nova.volume.san below
    def _execute(self, *cmd):
        command = ['ssh', self.host]
        command.extend(*cmd)
        return subprocess.check_output(command)
    
    def _build_volume_name(self, volume_id):
        return "volume-%08x" % volume_id

    def _build_zfs_poolname(self, volume_id):
        return "dpool/%s" % self._build_volume_name(volume_id)

    def _build_zvol_name(self, volume_id):
        pool = self._build_zfs_poolname(volume_id)
        return '/dev/zvol/rdsk/%s' % pool
        
    def _get_luid(self, volume_id):
        zvol_name = self._build_zvol_name(volume_id)
        out = self._execute('/usr/sbin/sbdadm', 'list-lu')
        lines = [ line.strip() for line in out.splitlines() ]
        # Strip headers
        if len(lines) >= 1:
            if lines[0] == '':
                lines = lines[1:]
        if len(lines) >= 4:
            assert 'Found' in lines[0]
            assert '' == lines[1]
            assert 'GUID' in lines[2]
            assert '------------------' in lines[3]
            lines = lines[4:]
        for line in lines:
            items = line.split()
            assert len(items) == 3
            if items[2] == zvol_name:
                luid = items[0].strip()
                return luid
        
    def _is_lu_created(self, volume_id):
        return self._get_luid(volume_id)

    def _build_iscsi_target_name(self, volume_id):
        name = self._build_volume_name(volume_id)
        return "%s%s" % ("iqn.2010-10.org.openstack:", name)

    def _get_prefixed_values(self, data, prefix):
        """Collect lines which start with prefix; with trimming"""
        matches = []
        for line in data.splitlines():
            line = line.strip()
            if line.startswith(prefix):
                match = line[len(prefix):]
                match = match.strip()
                matches.append(match)
        return matches

    def _get_target_groups(self):
        out = self._execute('/usr/sbin/stmfadm', 'list-tg')
        return self._get_prefixed_values(out, 'Target group: ')    

    def _target_group_exists(self, target_group_name):
        return target_group_name not in self._get_target_groups()

    def _get_target_group_members(self, tg_name):
        out = self._execute('/usr/sbin/stmfadm', 'list-tg', '-v', tg_name)
        return self._get_prefixed_values(out, 'Member: ')

    def _is_target_group_member(self, target_group_name, iscsi_target_name):
        members =  self._get_target_group_members(target_group_name)
        return iscsi_target_name in members

    def _get_iscsi_targets(self):
        out = self._execute('/usr/sbin/itadm', 'list-target')
        matches = [ line.strip() for line in out.splitlines() ]
        # Skip header
        if len(matches) != 0:
            assert 'TARGET NAME' in matches[0]
            matches = matches[1:]
        targets = []
        for line in matches:
            items = line.split()
            assert len(items) == 3
            targets.append(items[0])
        return targets

    def _iscsi_target_exists(self, iscsi_target_name):
        return iscsi_target_name in self._get_iscsi_targets()

    def _view_exists(self, luid):
        out = self._execute('/usr/sbin/stmfadm', 'list-view', '-l', luid)
        if "no views found" in out:
            return False
        if "View Entry:" in out:
            return True

    def create_export(self, volume):
        """Creates an export for a logical volume."""
        zvol_name = self._build_zvol_name(volume.id)
        if not self._is_lu_created(volume.id):
           self._execute(['/usr/sbin/sbdadm', 'create-lu', zvol_name]) 
        luid = self._get_luid(volume.id)
        iscsi = self._build_iscsi_target_name(volume.id)
        volume_name = self._build_volume_name(volume.id)
        tg_name = 'tg-%s' % volume_name
        if not self._target_group_exists(tg_name):
            self._execute(['/usr/sbin/stmfadm', 'create-tg', tg_name])
        if not _is_target_group_member(tg_name, iscsi):
            self._execute('/usr/sbin/stmfadm', 'add-tg-member', '-g', tg_name,
                          iscsi)
        if not self._iscsi_target_exists(iscsi):
            self._execute('/usr/sbin/itadm', 'create-target', '-n', iscsi) 
        if not _view_exists(luid):
            self._execute('/usr/sbin/stmfadm', 'add-view', '-t', tg_name, luid)

    def remove_export(self, volume):
        """Removes an export for a logical volume."""
        luid = self._get_luid(volume.id)
        iscsi = self._build_iscsi_target_name(volume.id)
        volume_name = self._build_volume_name(volume.id)
        tg_name = 'tg-%s' % volume_name
        if self._view_exists(luid):
            self._execute('/usr/sbin/stmfadm', 'remove-view', '-l', luid, '-a')
        if self._iscsi_target_exists(iscsi):
            self._execute('/usr/sbin/stmfadm', 'offline-target', iscsi)
            self._execute('/usr/sbin/itadm', 'delete-target', iscsi)
        # We don't delete the tg-member; we delete the whole tg!
        if self._target_group_exists(tg_name):
            self._execute('/usr/sbin/stmfadm', 'delete-tg', tg_name)
        if self._is_lu_created(volume):
            self._execute('/usr/sbin/sbdadm', 'delete-lu', luid)

class Volume(object):
    """Controller for volume information."""
    def __init__(self, id):
        self.id = int(id)
        data = self._get_db_data(id)
        self.host = data['host']
        self.size = int(data['size'])
        self.display_name = data['display_name']
        self.user_id = data['user_id']
        self.db_status = data['status']

    def _get_db_data(self, id):
        cmd = ['ssh', 'm3-p', './get_volume', id]
        txt = subprocess.check_output(cmd)
        rows = _tbl_text(txt) 
        return rows[0] 

    def status(self, servers):
        columns = ["ID", "Display Name", "Size", "User ID", "DB Status"]
        column_renames = {}
        formatters = {}
            
        def format_snaps(server):
            def snaps (volume):
                snapshots = server.volume_migrate_snapshots(volume)
                return ", ".join([ s['NAME'] for s in snapshots ])
            return snaps

        for server in servers:
            column_name = server.host
            if server.service_host == self.host:
                column_name += " (Main)"
            columns.append(column_name)
            formatters[column_name] = format_snaps(server)
        
        align = max(map(len, columns))
        for column in columns:
            attr = column.lower().replace(' ', '_')
            if column in column_renames:
                attr = column_renames[column] 
            if column in formatters:
                text = formatters[column](self)
            else:
                text = getattr(self, attr, "")
            print "%s: %s" % (string.rjust(column, align), text)

    def is_locked(self):
        return True if self.db_status == 'deleted' else False

    def lock(self):
        id = str(self.id)
        cmd = ['ssh', 'm3-p', './reset_volume_state', 'deleted', id]
        subprocess.check_call(cmd)

    def unlock(self):
        id = str(self.id)
        cmd = ['ssh', 'm3-p', './reset_volume_state', 'detached', id]
        subprocess.check_call(cmd)
        
    def stage(self, source, dest, skip_snapshot=False):
        current_host = self.host
        if not source:
            Exception("Unknown server for %s" % (self.host))
        if not dest:
            Exception("Unknown server for %s" % (args.destionation))
        snap_name = None
        if skip_snapshot:
            snap_name = source.max_snapshot_name(self)
        else:
            snap_name = source.snapshot(self)
        if not snap_name:
            Exception("Failed to create snapshot on %s" % (self.host))
        source.send_snapshot(dest, self, snap_name)

    def _update_exports(self, source, dest):
        """Destory source and construct dest iscsi exports; update the db."""
        source.remove_export(self)
        source.create_export(self)
        # Update the database
        host = dest.service_host
        iqn = '"%s"' % dest.volume_iqn(self)
        cmd = ['ssh', 'm3-p', './volume_migrate', str(self.id), host, iqn]
        
    def migrate(self, dest, skip_transfer=False):
        if not self.is_locked():
            Exception("You must lock a volume before migrating it.")
        destination_snapshots = dest.volume_migrate_snapshots(self)
        if skip_transfer and len(destination_snapshots) == 0:
            Exception("You must send a snapshot before you can migrate.")
        source = self.servers.get_server(self.host)
        if not skip_transfer:
            self.stage(source, dest)
        self._update_exports(source, dest)

    def _build_snapshot_range(self, snapshots):
        snapshots = [s['NAME'] for s in snapshots]
        volume, _ = snapshots[0].split("@")
        snapshots = [s.split("@")[1] for s in snapshots]
        return "%s@%s" % (volume, ",".join(snapshots))

    def cleanup(self, servers, do_snapshots=False, do_volume=False):
        if do_snapshots:
            for server in servers:
                snapshots = server.volume_migrate_snapshots(self)
                if len(snapshots) == 0:
                    continue
                snapshot_range = self._build_snapshot_range(snapshots)     
                server.destroy(snapshot_range)
        if do_volume:
            # volume deletion not implemented right now
            pass

    def notify(self, message=None):
        keystone = get_keystone_client()
        user = keystone.users.get(self.user_id)
        if not user:
           Exception("Unknown user for volume, user_id: %s" % self.user_id)
        if not message:
            template = """\
# Enter the message you would like to send to the volume owner.
# Note that lines beginning with a '#' will be removed.
# Details on this volume:

# ID : %d
# Name : %s
# Owner-Name : %s
# Owner-Email : %s
# Size : %d
"""
            initial = template % (self.id, self.display_name, user.name,
                                  user.email, self.size)
            message = get_text_from_editor(initial)
        sender = "magellan-support@mcs.anl.gov"
        subject_template = "Magellan: Notice for volume: '%s' (%d)"
        subject = subject_template % (self.display_name, self.id)
        send_mail(sender, [user.email], subject, message)

class MigrateShell(Shell):
    
    def __init__(self, servers=[]):
        self.servers = servers

    @arg('volume', help='Volume ID')
    def do_status(self, args):
        """Determine the status of a volume."""
        Volume(args.volume).status(servers=self.servers.all())
    
    @arg('volume', help='Volume ID')
    def do_lock(self, args):
        """Prevent a user from attaching it. (Mark as deleted.)"""
        volume = Volume(args.volume)
        volume.lock()

    @arg('volume', help='Volume ID')
    def do_unlock(self, args):
        """Mark the volume as available."""
        volume = Volume(args.volume)
        volume.unlock()

    @arg('volume', help='Volume ID')
    @arg('destination', help='Hostname of the destination machine')
    @arg('--skip', action='store_true',
         help='Skip creating another snapshot; just send the most recent one.')
    def do_stage(self, args):
        """Snapshot the volume and transfer that snapshot."""
        volume = Volume(args.volume)
        source = self.servers.get_server(volume.host)
        dest = self.servers.get_server(args.destination)
        volume.stage(source, dest, skip_snapshot=args.skip)

    @arg('volume', help='Volume ID')
    @arg('destination', help='Hostname of the destination machine')
    @arg('--skip', action='store_true',
         help='Skip the snapshot + trasfer step.')
    def do_migrate(self, args):
        """Transfer final snapshot and set dest as primary server."""
        volume = Volume(args.volume)
        dest = self.servers.get_server(args.destination)
        volume.migrate(dest, skip_transfer=args.skip)

    @arg('volume', help='Volume ID')
    @arg('-m', '--message', type=str, help='Message to send to the user')
    def do_notify(self, args):
        """Send the volume owner a message."""
        volume = Volume(args.volume)
        volume.notify(message=args.message)
        
    @arg('volume', help='Volume ID')
    @arg('-d', '--delete', action='store_true',
         help='Delete the non-primary volume.')
    def do_cleanup(self, args):
        """Clean up snapshots on non-primary volume servers."""
        servers = self.servers.all()
        volume = Volume(args.volume)
        volume.cleanup(servers, do_snapshots=True, do_volume=args.delete)

    def do_servers(self, args):
        """List configured servers."""
        servers = self.servers.all()
        for server in servers:
            print server.host
        
def main():
    v1 = VolumeServer(host='v1-p', service_host='vol01', migrate_iface='v1-i')
    v3 = VolumeServer(host='v3-p', service_host='v3', migrate_iface='v3-i')
    servers = VolumeServerContainer(servers=[v1,v3])
    shell = MigrateShell(servers=servers)
    shell.main(sys.argv[1:])

if __name__ == '__main__':
    main()
