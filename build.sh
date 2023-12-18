#!/bin/bash

export CLEAN=
export HOST=

check_ivm64_env(){
    if ! which ivm64-gcc; then
        echo "ivm64-gcc compiler not found in PATH"
        exit 1
    fi
    ivm_fsgen=(${IVM_FSGEN:-ivm64-fsgen})
    if ! which "${ivm_fsgen[0]}"; then
        echo "Neither in-memory filesystem generator ivm64-fsgen found in PATH"
        echo "nor IVM_FSGEN environment variable has a valid filesystem generator"
        echo "Examples: export IVM_FSGEN=ivm64-fsgen"
        echo "          export IVM_FSGEN='ivmfs-gen.sh'"
        exit 1
    fi
    ivm_emu=(${IVM_EMU:-ivm64-emu})
    if ! which "${ivm_emu[0]}"; then
        echo "$ivm_emu: IVM_EMU environment variable must have a valid ivm emulator"
        echo "Examples: export IVM_EMU=ivm64-emu"
        echo "          export IVM_EMU='ivm run'"
        exit 1
    fi
    ivm_as=(${IVM_AS:-ivm64-as})
    if ! which "${ivm_as[0]}"; then
        echo "IVM_AS environment variable must have a valid ivm emulator"
        echo "Examples: export IVM_AS=ivm64-as"
        echo "          export IVM_AS='ivm as'"
        exit 1
    fi
}

while test $# -gt 0
do
    if test "$1" == '-c'; then
        CLEAN=1
    elif test "$1" == 'linux'; then
        HOST=linux
    elif [[ "$1" =~ ^ivm ]] ; then
        HOST=ivm64
        check_ivm64_env
    fi
    shift
done

if test -z "$HOST" -a -z "$CLEAN"; then
    1>&2 echo "Usage: $0 arch    #make arch "
    1>&2 echo "       $0 -c arch #clean before making arch"
    1>&2 echo "       $0 -c      #clean all buildings"
    1>&2 echo "       arch: select either 'linux' or 'ivm64' target architecture" 
    exit 1
fi

nproc=8

if test -n "$CLEAN" -a -z "$HOST"; then
    HOST=linux make clean
    HOST=ivm64 make clean
elif test -n "$CLEAN" -a -n "$HOST"; then
    HOST=$HOST make clean
    HOST=$HOST make -j${nproc}
else
    HOST="$HOST" make -j${nproc}
fi
