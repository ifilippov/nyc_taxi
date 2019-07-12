fn=nyctaxi-`date +$(hostname)-%y%m%d-%H%M`
(
for run in 1 2 3; do
  for cpu in "" "numactl --cpunodebind 0" "taskset -c 24-35" "taskset -c 12-35"; do
    for i in 65536 32768 24576 20480 16384 14336 12288 10240 8192 6144 4096 3072 2048 1024 683 640 512 384; do
      for bench in ../build/bench_csv*; do
        echo "Block: ${i}K; $cpu $bench"
        $cpu $bench ../*.csv `$cpu nproc` $((i*1024))
done; done; done; done
) | tee $fn.log
sed -rz "s/Block: ([^;]+); ([^\n]*)\\nThread number: ([0-9]+); Block size: ([0-9]+)\\n[^\\n]+\\nDONE! [0-9]+ seconds . ([0-9]+) milliseconds\\n/$fn,\\1,\\4,\"\\2\",\\3,\\5/g" $fn.log >$fn.csv
