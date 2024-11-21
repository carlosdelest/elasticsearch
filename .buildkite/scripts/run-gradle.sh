#!/bin/bash
# drop page cache and kernel slab objects on linux
GRADLEW="./gradlew --console=plain --parallel --build-cache --no-watch-fs -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/ -Dorg.elasticsearch.build.cache.push=true -Ddevelocity.deprecation.captureOrigin=true"

[[ -x /usr/local/sbin/drop-caches ]] && sudo /usr/local/sbin/drop-caches
rm -Rfv ~/.gradle/init.d
mkdir -p ~/.gradle/init.d && cp -v .buildkite/init.gradle ~/.gradle/init.d
if [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then
   MAX_WORKERS=16
elif [ -f /proc/cpuinfo ]; then
   MAX_WORKERS=`grep '^cpu\scores' /proc/cpuinfo  | uniq | sed 's/\s\+//g' |  cut -d':' -f 2`
else
   if [[ "$OSTYPE" == "darwin"* ]]; then
      MAX_WORKERS=`sysctl -n hw.physicalcpu | sed 's/\s\+//g'`
      echo "l16 MAX_WORKERS $MAX_WORKERS"
   else
      echo "Unsupported OS Type: $OSTYPE"
      exit 1
   fi
fi
if pwd | grep -v -q ^/dev/shm ; then
   echo "Not running on a ramdisk, reducing number of workers"
   MAX_WORKERS=$(($MAX_WORKERS*2/3))
fi

# Export glibc version as environment variable since some BWC tests are incompatible with later versions
export GLIBC_VERSION=$(ldd --version | grep '^ldd' | sed 's/.* \([1-9]\.[0-9]*\).*/\1/')

MAX_WORKERS=${GRADLE_MAX_WORKERS:-$MAX_WORKERS}

set -e
$GRADLEW -S --max-workers=$MAX_WORKERS $@
