#!/usr/local/lib/python2.7.9/bin/python
##!/usr/bin/env python
import string
import random
import argparse
import ConfigParser
#from configparser import ConfigParser
import os
import subprocess
import sys
from keystoneclient.exceptions import NotFound
from keystoneclient.exceptions import Conflict
from keystoneclient.v2_0 import client

import urllib3
#import logging
#logging.basicConfig(level=logging.DEBUG)

def create_tenant_map(tenants):
	tenants_map = {}
	for tenant in tenants:
		tenants_map[tenant.name] = tenant

	return tenants_map

def create_user_map(users):
	user_map = {}
	for user in users:
		user_map[user.name] = user

	return user_map

def create_role_map(roles):
	role_map = {}
	for role in roles:
		role_map[role.name] = role

	return role_map

def get_tenants_for_user(user, tenants):
	retVal = []
	for tenant in tenants:
		users_for_tenant = tenant.list_users()
		for user_in_tenant in users_for_tenant:
			if user.name == user_in_tenant.name:
				retVal.append(tenant)
				break
			else:
				print("user %s not found in %s, but %s was " % (user.name, tenant.name, user_in_tenant.name))
	return retVal		
		
def main():
	havana = client.Client( username = 'root', 
		password = 'b0z7fIcg7RTnMifkUd',
		tenant_name = 'Support',
		auth_url = 'https://havana.cloud.mcs.anl.gov:35357/v2.0')

	kilo = client.Client( username = 'apiuser',
		password = 'clddev',
		tenant_name = 'admin',
		auth_url = 'http://10.3.0.3:35357/v2.0')

	h_roles = havana.roles.list()
	k_roles = kilo.roles.list()
	#print(k_roles)
	for h_role in h_roles:
		#print(dir(kilo.roles))
		try:
			kilo.roles.create(h_role.name)
			print("Adding: %s" % h_role.name)
		except Conflict:
			print("Name already existed: %s" % h_role.name)

	k_role_map = create_role_map(kilo.roles.list())
	h_role_map = create_role_map(havana.roles.list())

	h_tenants = havana.tenants.list()
	k_tenants = kilo.tenants.list()
	for h_tenant in h_tenants:
		#print(dir(h_tenant))
		try:
			kilo.tenants.create(tenant_name=h_tenant.name,
				description=h_tenant.description,
				enabled = h_tenant.enabled)
			print("Adding: %s" % h_tenant.name)
		except Conflict:
			print("Tenant already existed: %s" % h_tenant.name)

	h_users = havana.users.list()
	k_users = kilo.users.list()
	k_tenants = kilo.tenants.list() #needs to be refreshed after the above
	k_user_map = create_user_map(k_users)
	for h_user in h_users:
		k_user = None
		try:
			k_user = kilo.users.create(name=h_user.name,
				password=''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8)),
				email = h_user.email,
				enabled = h_user.enabled)
			print("Created user %s" % h_user.name)
		except Conflict:
			k_user = k_user_map[h_user.name]
			print("User %s already existed" % h_user.name)

	k_user_map = create_user_map(k_users)
	k_tenant_map = create_tenant_map(k_tenants)
	for h_tenant in h_tenants:
		h_users_for_tenant = h_tenant.list_users()
		k_tenant = k_tenant_map[h_tenant.name]
		for h_tenant_user in h_users_for_tenant:
			try:
				for h_role in h_tenant_user.list_roles(h_tenant):
					k_tenant.add_user(k_user_map[h_tenant_user.name], k_role_map[h_role.name])
					print("added %s to tenant %s as %s" % (h_tenant_user.name, k_tenant.name, h_role.name))
			except Conflict:
				print("%s already in tenant %s" % (h_tenant_user.name, k_tenant.name))
	

def apply_settings_for_os_env(cfg, args):
    for k,v in cfg.iteritems():
        os.environ[k] = v
    keystone = client.Client(
        username =    cfg.get("username"),
        password =    cfg.get("password"),
        tenant_name = cfg.get("tenant_name"),
        auth_url =    cfg.get("auth_url"))
    # Get standard admin and member roles:
    admin_role_name = os.environ.get('admin_role', 'admin')
    member_role = [r for r in keystone.roles.list() if r.name == "Member"][0]
    admin_role  = [r for r in keystone.roles.list() if
                   r.name == admin_role_name][0] 
    if args.delete:
        delete_user(keystone, args)
    else:
        ensure_tenant(keystone, args)
        ensure_user(keystone, args)
        if args.tenant != None:
            update_user_tenant(keystone, args, admin_role=admin_role, member_role=member_role)

def get_user (name, keystone):
    matched = [u for u in keystone.users.list() if u.name == name]
    return matched[0] if len(matched) else None

def delete_user(keystone, args):
    user = get_user(args.user, keystone) 
    user.delete()

def ensure_tenant(keystone, args):
    if not args.tenant:
        return
    matched = [ tenant for tenant in keystone.tenants.list()
        if tenant.name == args.tenant ]
    if len(matched):
        return matched[0]
    else:
        return keystone.tenants.create(args.tenant)

def ensure_user(keystone, args):
    user = get_user(args.user, keystone)
    # If no user was found, create the user with password and email
    if user is None:
        if args.password is None:
            print("User not found and password is not supplied!")
            sys.exit(1)
        keystone.users.create(
            name     = args.user,
            password = args.password,
            email    = args.email,
            enabled  = False if args.disable else True
        )
    else:
        keystone.users.update_enabled(user, False if args.disable else True)
        if args.password:
            keystone.users.update_password(user, args.password)
        if args.email:
            keystone.users.update(user, email=args.email)

def update_user_tenant(keystone, args, admin_role=None, member_role=None):
    user = get_user(args.user, keystone)
    tenant = [ t for t in keystone.tenants.list() if t.name == args.tenant ][0]
    if tenant is None:
        # Exit with an error if we couldn't find the tenant
        print("Tenant not found!")
        sys.exit(1)
    # Remove the admin role for the user if we did not set the admin flag
    #if not args.is_admin:
    #    try:
    #        keystone.roles.remove_user_role(user, admin_role, tenant=tenant)
    #    except Exception:
    #        pass
    if args.evict:
        # Evicting the user from the tenant
        try:
            keystone.roles.remove_user_role(user, admin_role, tenant=tenant)
        except NotFound:
            pass
        try:
            keystone.roles.remove_user_role(user, member_role, tenant=tenant)
        except NotFound:
            pass
    else:
        # Adding the user to the tenant with the correct role
        role = admin_role if args.is_admin else member_role
        ub_user = os.environ.get('username')
        ub_pass = os.environ.get('password')
        ub_tenant = os.environ.get('tenant_name')
        auth_url = os.environ.get('auth_url')
        try:
            subprocess.check_output(["keystone", "--os_username", ub_user, "--os_password", ub_pass,
                                     "--os_tenant_name", ub_tenant, "--os_auth_url", auth_url,
                                     "user-role-add", "--user", user.id, "--tenant_id", tenant.id,
                                     "--role", role.id])
        except Exception as e:
            print("Error adding %s user to %s tenant: %s" % (user.name, tenant.name, e))
            sys.exit(1)

if __name__ == '__main__':
    main()
