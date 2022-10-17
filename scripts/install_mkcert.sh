#! /bin/bash

DIR="$1"

if [[ "$DIR" == "" ]]; then
    DIR="/tmp/mkcert"
fi

git clone https://github.com/FiloSottile/mkcert "$DIR" && cd "$DIR"
go build -ldflags "-X main.Version=$(git describe --tags)"
cp mkcert ~/bin
