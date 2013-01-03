#!/bin/bash
CWD=`pwd`
FLAVOR=$1

if [[ -z $FLAVOR ]] ; then
    echo "$0 <flavor> <reclone?>";
    exit 1;
fi
if [[ ! -d $CWD/$FLAVOR ]] ; then 
    echo "No configuration for $FLAVOR found!";
    exit 1;
fi
# Ensure that git is installed
apt-get update -qq
apt-get install -qqy git
# Check out the devstack repo if it doesn't already exist
if [[ ! -d $CWD/devstack ]] ; then
    git clone git://github.com/openstack-dev/devstack.git
fi
# Get / set the devstack branch to use
DEVSTACK_BRANCH=`cat $CWD/$FLAVOR/devstack_branch`
if [[ -z $DEVSTACK_BRANCH ]] ; then
    DEVSTACK_BRANCH=master
fi
pushd devstack
    git checkout $DEVSTACK_BRANCH
popd
# Copy all configuration into the devstack
cp -r $CWD/$FLAVOR/conf/* devstack
pushd devstack
    echo $FLAVOR > current_flavor
    . stack.sh
popd
