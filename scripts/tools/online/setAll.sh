echo "get -s -d it.unimi.di.law.bubing -b name=$(hostname),type=Agent $1" |java -jar jmxterm.jar -l localhost:9999 -v silent -n
echo "set -d it.unimi.di.law.bubing -b name=$(hostname),type=Agent $1 $2" |java -jar jmxterm.jar -l localhost:9999 -v silent -n
echo "get -s -d it.unimi.di.law.bubing -b name=$(hostname),type=Agent $1" |java -jar jmxterm.jar -l localhost:9999 -v silent -n
