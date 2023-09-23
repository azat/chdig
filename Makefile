# FIXME: rewrite with build.rs

debug ?=

# Version normalization for deb/rpm:
# - trim "v" prefix
# - first "-" replace with "+"
# - second "-" replace with "~"
#
# Refs: https://www.debian.org/doc/debian-policy/ch-controlfields.html
CHDIG_VERSION=$(shell git describe | sed -e 's/^v//' -e 's/-/+/' -e 's/-/~/')
# Refs: https://wiki.archlinux.org/title/Arch_package_guidelines#Package_versioning
CHDIG_VERSION_ARCH=$(shell git describe | sed -e 's/^v//' -e 's/-/./g')

$(info DESTDIR = $(DESTDIR))
$(info CHDIG_VERSION = $(CHDIG_VERSION))
$(info CHDIG_VERSION_ARCH = $(CHDIG_VERSION_ARCH))
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

build_completion:
	cargo run -- --completion bash > dist/chdig.bash-completion

install:
	cp target/$(target)/chdig $(DESTDIR)/bin/chdig$(extension)
	cp dist/chdig-tfg $(DESTDIR)/bin/chdig-tfg

packages: build build_completion deb rpm archlinux

deb: build
	CHDIG_VERSION=${CHDIG_VERSION} nfpm package --config chdig-nfpm.yaml --packager deb
rpm: build
	CHDIG_VERSION=${CHDIG_VERSION} nfpm package --config chdig-nfpm.yaml --packager rpm
archlinux: build
	CHDIG_VERSION=${CHDIG_VERSION_ARCH} nfpm package --config chdig-nfpm.yaml --packager archlinux

all: build build_completion install

help:
	@echo "Usage: make [debug=1]"
