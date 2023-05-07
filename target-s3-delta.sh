unset VIRTUAL_ENV

STARTDIR=$(pwd)
TOML_DIR=$(dirname "$0")

cd "$TOML_DIR" || exit
poetry install 1>&2
poetry run target-s3-delta $* < /dev/stdin