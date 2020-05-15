#! /bin/bash

devices=`adb devices | grep 'device$' | cut -f1`
pids=""

for device in $devices
do
    log_file="$device-`date +%d-%m-%H:%M:%S`.log"
    echo "Logging device $device to \"$log_file\""
    adb -s $device logcat -v threadtime > $log_file &
    pids="$pids $!"
done

echo "Children PIDs: $pids"

killemall()
{
    echo "Killing children (what a shame...)"

    for pid in $pids
    do
        echo "Killing $pid"
        kill -TERM $pid
    done
}

trap killemall INT

wait
