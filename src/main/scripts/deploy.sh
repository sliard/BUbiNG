#!/bin/zsh

[[ -x =xmlstarlet ]] || {
	echo "install xmlstarlet"
	exit 1
}

[[ -e pom.xml ]] || {
	ant ivy-pom
	[[ -f pom.xml ]] || {
		echo "can't generate pom.xml"
		exit 2
	}
}


GROUP_ID=$(xmlstarlet sel -N my=http://maven.apache.org/POM/4.0.0  -t -v  "/my:project/my:groupId" pom.xml)
ARTIFACT_ID=$(xmlstarlet sel -N my=http://maven.apache.org/POM/4.0.0  -t -v  "/my:project/my:artifactId" pom.xml)
VERSION=$(xmlstarlet sel -N my=http://maven.apache.org/POM/4.0.0  -t -v  "/my:project/my:version" pom.xml)
PACKAGING=$(xmlstarlet sel -N my=http://maven.apache.org/POM/4.0.0  -t -v  "/my:project/my:packaging" pom.xml)
FILE=./${ARTIFACT_ID}-${VERSION}.jar
SRC_FILE=./${ARTIFACT_ID}-${VERSION}.jar
REPOSITORY_ID=exensa_local
URL=scpexe://maven.babbar.eu/var/http/maven-repository/releases

[[ -e ${SRC_FILE} ]] || ant srcdist
[[ -e ${FILE} ]] || ant jar
[[ -e ${FILE} ]] && {
  mvn -f deploy.xml deploy:deploy-file -DgroupId=${GROUP_ID} \
        -DartifactId=${ARTIFACT_ID} \
        -Dversion=${VERSION} \
        -Dpackaging=${PACKAGING} \
        -Dfile=${FILE} \
        -DrepositoryId=${REPOSITORY_ID} \
        -Durl=${URL}

  mvn -f deploy.xml deploy:deploy-file -DgroupId=${GROUP_ID} \
        -DartifactId=${ARTIFACT_ID} \
        -Dversion=${VERSION} \
        -Dpackaging=java-source \
        -DgeneratePom=false \
        -Dfile=${FILE} \
        -Dsources=${SRC_FILE} \
        -DrepositoryId=${REPOSITORY_ID} \
        -Durl=${URL}
}

