#!/usr/bin/env bash
#if [ ! -f "$1" ]; then
#    echo "Usage: . test.sh [/path/to/manage.py]";
#    return 1;
#fi
set -e
rm -f .coverage
echo "Running Unit Tests..."
coverage run -a --rcfile=coverage.ini -m unittest -v uit_plus_job.tests.unit_tests.test_oauth2

echo "Unit Tests Coverage Report..."
coverage report -m
echo "Linting..."
flake8
echo "Testing Complete"

