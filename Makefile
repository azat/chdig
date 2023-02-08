# FIXME: rewrite with build.rs

debug ?=
CHDIG_SHA=$(shell git describe --always)
export CHDIG_SHA

$(info DESTDIR = $(DESTDIR))
$(info CHDIG_SHA = $(CHDIG_SHA))
$(info debug = $(debug))

ifdef debug
  release :=
  target := debug
  extension := -debug
else
  release := --release
  target := release
  extension :=
endif

.PHONY: build tfg chdig install deb rpm archlinux packages

chdig:
	cargo build $(release)

tfg:
	pyinstaller contrib/tfg/tfg.py --onefile
	ln -r -f -s dist/tfg dist/chdig-tfg

build: chdig tfg

install:
	cp target/$(target)/chdig $(DESTDIR)/bin/chdig$(extension)
	cp dist/chdig-tfg $(DESTDIR)/bin/chdig-tfg

packages: build deb rpm archlinux

deb: build
	nfpm package --config chdig-nfpm.yaml --packager deb
rpm: build
	nfpm package --config chdig-nfpm.yaml --packager rpm
archlinux: build
	nfpm package --config chdig-nfpm.yaml --packager archlinux

all: build install

help:
	@echo "Usage: make [debug=1]"
