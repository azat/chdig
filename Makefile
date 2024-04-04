# FIXME: rewrite with build.rs

debug ?=
target ?= $(shell rustc -vV | sed -n 's|host: ||p')
arch = $(shell rustc -Z unstable-options --print target-spec-json | jq -r .arch)

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
else
  cargo_build_opts := --release
  target_type = release
endif

ifneq ($(target),)
    cargo_build_opts += --target $(target)
endif

export PYO3_CONFIG_FILE = $(CURDIR)/contrib/flameshow/build/pyo3-build-config-file-$(target).txt
$(info PYO3_CONFIG_FILE = $(PYO3_CONFIG_FILE))

.PHONY: build build_completion deploy-binary chdig install run \
	deb rpm archlinux tar packages

# This should be the first target (since ".DEFAULT_GOAL" is supported only since 3.80+)
default: build
.DEFAULT_GOAL: default

$(PYO3_CONFIG_FILE):
	env -u PYO3_CONFIG_FILE cargo build $(cargo_build_opts) -p flameshow

chdig: $(PYO3_CONFIG_FILE)
	cargo build $(cargo_build_opts)

run: chdig
	cargo run $(cargo_build_opts)

build: chdig deploy-binary

build_completion: chdig
	cargo run $(cargo_build_opts) -- --completion bash > target/chdig.bash-completion

install: chdig build_completion
	install -m755 -D -t $(DESTDIR)/bin target/$(target)/$(target_type)/chdig
	install -m644 -D -t $(DESTDIR)/share/bash-completion/completions target/chdig.bash-completion

deploy-binary: chdig
	cp target/$(target)/$(target_type)/chdig target/chdig

packages: build build_completion deb rpm archlinux tar

deb: build
	CHDIG_VERSION=${CHDIG_VERSION} nfpm package --config chdig-nfpm.yaml --packager deb
rpm: build
	CHDIG_VERSION=${CHDIG_VERSION} nfpm package --config chdig-nfpm.yaml --packager rpm
archlinux: build
	CHDIG_VERSION=${CHDIG_VERSION_ARCH} nfpm package --config chdig-nfpm.yaml --packager archlinux
.ONESHELL:
tar: archlinux
	CHDIG_VERSION=${CHDIG_VERSION_ARCH} nfpm package --config chdig-nfpm.yaml --packager archlinux
	tmp_dir=$(shell mktemp -d /tmp/chdig-${CHDIG_VERSION}.XXXXXX)
	echo "Temporary directory for tar package: $$tmp_dir"
	tar -C $$tmp_dir -vxf chdig-${CHDIG_VERSION_ARCH}-1-x86_64.pkg.tar.zst usr
	# Strip /tmp/chdig-${CHDIG_VERSION}.XXXXXX and replace it with chdig-${CHDIG_VERSION}
	# (and we need to remove leading slash)
	tar --show-transformed-names --transform "s#^$${tmp_dir#/}#chdig-${CHDIG_VERSION}-${arch}#" -vczf chdig-${CHDIG_VERSION}-${arch}.tar.gz $$tmp_dir
	echo rm -fr $$tmp_dir

help:
	@echo "Usage: make [debug=1] [target=<TRIPLE>]"
