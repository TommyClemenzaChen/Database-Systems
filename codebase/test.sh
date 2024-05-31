#!/bin/bash - 

unzip $1.zip
if [ $? -ne 0 ]; then
    echo "[ERROR] ZIP File Error. Please fix it!"
    echo
    exit 1
fi

cd $1
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi

cd codebase
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi

cd qe
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi

make clean
make

./qetest_01
./qetest_02
./qetest_03
./qetest_04
./qetest_05
./qetest_06
./qetest_09
./qetest_10
