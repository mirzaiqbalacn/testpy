#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# Assumes in the tests/ directory

echo "Checking our configuration option appears in help"
flake8 -h 2>&1 | grep "black-config"

set +o pipefail

echo "Checking we report an error when can't find specified config file"
flake8 --black-config does_not_exist.toml 2>&1 | grep -i "could not find"

echo "Checking failure with mal-formed TOML file"
flake8 --select BLK test_cases/ --black-config with_bad_toml/pyproject.toml 2>&1 | grep -i "could not parse"

set -o pipefail

echo "Checking we report no errors on these test cases"
# Must explicitly include *.pyi or flake8 ignores them
flake8 --select BLK test_cases/*.py*
# Adding --black-config '' meaning ignore any pyproject.toml should have no effect:
flake8 --select BLK PytF/*.py --black-config ''
flake8 --select BLK --max-line-length 50 PytF/*.py
flake8 --select BLK --max-line-length 90 PytF/*.py

# Adding --black-config '' should have no effect:
#flake8 --select BLK --max-line-length 88 with_pyproject_toml/ --black-config ''
flake8 --select BLK PytF/*.py

# Here using --black-config '' meaning ignore any (bad) pyproject.toml files:
flake8 --select BLK PytF/*.py --black-config ''

echo "Tests passed."