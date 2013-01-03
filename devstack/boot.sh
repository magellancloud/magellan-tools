#!/bin/bash
CWD=`pwd`
if [[ -z $1 ]] ; then
    echo "$0 <flavor>";
    exit 1;
fi
if [ !-d $CWD/$1 ] ; then 
    echo "No configuration for $CWD/$1 found!";
    exit 1;
fi
# Ensure that git is installed
apt-get update
apt-get install -qqy git
# Clone the devstack repo and check out th
pushd $CWD/$1
    git clone git://github.com/openstack-dev/devstack.git
    pushd devstack
        git checkout stable/$1
    popd
    cp -r conf/* devstack
    pushd devstack
        ./stack.sh
    popd
popd
