#!/bin/bash

set -e
git pull

mvn clean install

VERSION=`grep -o -a -m 1 -h -r "version>.*</version" ./pom.xml | head -1 | sed "s/version//g" | sed "s/>//" | sed "s/<\///g"`
REPOSITORY_TYPE='release'
REPOSITORY_URL_TYPE=''

if [[ ${VERSION} = *"SNAPSHOT"* ]]; then
  REPOSITORY_TYPE="snapshot"
  REPOSITORY_URL_TYPE='snapshot'
fi

echo ""
echo "Deploying version: $VERSION ... to maven ${REPOSITORY_TYPE} repository"
echo ""

mvn deploy

echo ""
echo "Deploying hops-util-${VERSION}.jar to snurran.sics.se"
echo ""
scp target/hops-util-${VERSION}-shaded.jar glassfish@snurran.sics.se:/var/www/hops/hops-util-${VERSION}.jar


echo ""
echo "Building javadoc"
echo ""
mvn generate-sources javadoc:javadoc

echo ""
echo "Deploying hops-util-${VERSION}.jar javadoc to snurran.sics.se"
echo ""
rsync -r --delete target/site/apidocs/ glassfish@snurran.sics.se:/var/www/hops/hops-util-javadoc/${VERSION}

echo "Done!"
