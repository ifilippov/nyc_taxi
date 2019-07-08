(
for run in 1 2 3; do
  for cpu in "" "numactl --cpunodebind 0" "taskset -c 24-35" "taskset -c 12-35"; do
    for i in 1 4 8 9 10 11 12 13 14 15 16 18 20 24; do
      echo "Block: 8M/$((i))=$((8*1024/i))K; Tasks: $((8*1024*1024/i)); $cpu"
      $cpu ../build/bench ../*.csv $((8*1024*1024/i))
done; done; done
) | tee nyctaxi-`date +$(hostname)-%y%m%d-%H%M`.log
