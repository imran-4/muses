#!/bin/bash


export WORKSPACE=PATH_TO_WORKSPACE
export TOOLS_HOME=PATH_TO_COMMON_LIBRAARY_HOME
export MUSES_GIT_REPO=git://github.com/mi-1-0-0/muses.git


export JAVA_HOME=${TOOLS_HOME}/java/latest
export M3_HOME=${TOOLS_HOME}/maven/apache-maven-3.0.3
export PATH=$JAVA_HOME/bin:$M3_HOME/bin:$PATH

#Please uncomment these lines if the git repo needs to download
#git clone -o origin MUSES_GIT_REPO $WORKSPACE
#cd $WORKSPACE

M2DIR=`mktemp -d /tmp/muses-m2.XXXXX`
bin/mkdistro.sh $1 -Dmaven.repo.local=$M2DIR
EXIT=$?
rm -rf $M2DIR

exit $EXIT