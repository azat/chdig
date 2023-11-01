# FIXME: rewrite with build.rs

debug ?=
target ?=

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
$(info target = $(target))

ifdef debug
  cargo_build_opts :=
  target_type := debug
  extension := -debug
else
  cargo_build_opts := --release
  target_type = release
  extension :=
endif

ifneq ($(target),)
    cargo_build_opts += --target $(target)
endif

.PHONY: build tfg chdig install deb rpm archlinux packages

chdig:
	cargo build $(cargo_build_opts)

tfg:
	pyinstaller contrib/tfg/tfg.py --onefile
	ln -r -f -s dist/tfg dist/chdig-tfg

build: chdig tfg link

build_completion:
	cargo run -- --completion bash > dist/chdig.bash-completion

install:
	install -m755 -D -t $(DESTDIR)/bin target/$(target)/$(target_type)/chdig
	install -m755 -D -t $(DESTDIR)/bin dist/chdig-tfg
	install -m644 -D -t $(DESTDIR)/share/bash-completion/completions dist/chdig.bash-completion

link:
	cp target/$(target)/$(target_type)/chdig target/chdig

packages: build build_completion deb rpm archlinux

deb: build
	CHDIG_VERSION=${CHDIG_VERSION} nfpm package --config chdig-nfpm.yaml --packager deb
rpm: build
	CHDIG_VERSION=${CHDIG_VERSION} nfpm package --config chdig-nfpm.yaml --packager rpm
archlinux: build
	CHDIG_VERSION=${CHDIG_VERSION_ARCH} nfpm package --config chdig-nfpm.yaml --packager archlinux

all: build build_completion install

help:
	@echo "Usage: make [debug=1] [target=<TRIPLE>]"
