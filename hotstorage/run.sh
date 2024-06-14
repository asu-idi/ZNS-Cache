NAME=$1
# ./run.sh file-cache-bc-lru

LOGPATH=./micro-eval/$NAME

mkdir -p $LOGPATH

CONPATH=./configs/$NAME.json

TIME_SUFFIX="$(date '+%Y-%m-%d-%H-%M-%S')"

./opt/cachelib/bin/cachebench --json_test_config $CONPATH --progress_stats_file=$LOGPATH/$NAME-$TIME_SUFFIX-stat.log --logging=INFO &> $LOGPATH/$NAME-$TIME_SUFFIX-output.log
