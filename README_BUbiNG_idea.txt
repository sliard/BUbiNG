edit ant_ivy_bootstrap.sh and check ant_version to match http://www.apache.org/dist/ant/binaries/ (current version is 1.10.5)
run ant_ivy_bootstrap.sh and update ~/.bashrc or ~/.bash_profile with paths
run ant ivy-purge
run ant-ivy-setupjars
run ant ivy-pom
idea > File > New > Project from Existing Sources...
select pom.xml
  1. click Next
  2. uncheck ${ivy.pom.groupId}:${ivu.pom.artifactId}:${ivy.pom.version} then click Next
  3. select SDK 1.8 then click Next
  4. set Project name and click Finish
idea > View > Tool Windows > Ant Build
  1. click +
  2. select build.xml then click OK
idea > File > Project Structure...
  1. Project Settings > Project
    set Project language level to '8 - Lambdas, type annotations etc.'
  2. Project Settings > Modules > Dependencies
    remove all dependencies but '1.8 (java version 1.8.xxx)' and '<Module source>' using '-' to the right of the window
    add JARs or directories... > jars/compile using '+' to the right of the window
    add JARs or directories... > extjars using '+' to the right of the window
  3. Project Settings > Modules > Source
    verify that src is marked as Sources
    set Language level to '8 - Lambdas, type annotations etc.'
  4. Project Settings > Libraries
    remove all libraries
  5. click OK

