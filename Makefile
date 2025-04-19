debug ?=
target ?= $(shell rustc -vV | sed -n 's|host: ||p')
# Parse the target (i.e. aarch64-unknown-linux-musl)
target_os := $(shell echo $(target) | cut -d'-' -f3)
target_libc := $(shell echo $(target) | cut -d'-' -f4)
target_arch := $(shell echo $(target) | cut -d'-' -f1)
host_arch := $(shell uname -m)

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
$(info host_arch = $(host_arch))

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

# Normalize architecture names
norm_target_arch := $(shell echo $(target_arch) | sed -e 's/^aarch64$$/arm64/' -e 's/^x86_64$$/amd64/')
norm_host_arch := $(shell echo $(host_arch) | sed -e 's/^aarch64$$/arm64/' -e 's/^x86_64$$/amd64/')

$(info Normalized target arch: $(norm_target_arch))
$(info Normalized host arch: $(norm_host_arch))

# Cross compilation requires some tricks:
# - use lld linker
# - explicitly specify path for libstdc++
# (Also some packages, that you can found in github actions manifests)
#
# TODO: allow to use clang/gcc from PATH
ifneq ($(norm_host_arch),$(norm_target_arch))
  $(info Cross compilation for $(target_arch))

  # Detect the latest lld
  LLD := $(shell ls /usr/bin/ld.lld /usr/bin/ld.lld-* 2>/dev/null | sort -V | tail -n1)
  $(info LLD = $(LLD))
  # Detect the latest clang
  CLANG := $(shell ls /usr/bin/clang /usr/bin/clang-* 2>/dev/null | grep -e '/clang$$' -e '/clang-[0-9]\+$$' | sort -V | tail -n1)
  $(info CLANG = $(CLANG))
  CLANG_CXX := $(shell ls /usr/bin/clang++ /usr/bin/clang++-* 2>/dev/null | grep -e '/clang++$$' -e '/clang++-[0-9]\+$$' | sort -V | tail -n1)
  $(info CLANG_CXX = $(CLANG_CXX))

  export CC := $(CLANG)
  export CXX := $(CLANG_CXX)
  export RUSTFLAGS := -C linker=$(LLD)

  # /usr/aarch64-linux-gnu/lib64/ (archlinux aarch64-linux-gnu-gcc)
  prefix := /usr/$(target_arch)-$(target_os)-gnu/lib
  ifneq ($(wildcard $(prefix)),)
    export RUSTFLAGS := $(RUSTFLAGS) -C link-args=-L$(prefix)
  endif
  prefix := /usr/$(target_arch)-$(target_os)-gnu/lib64
  ifneq ($(wildcard $(prefix)),)
    export RUSTFLAGS := $(RUSTFLAGS) -C link-args=-L$(prefix)
  endif

  # /usr/lib/gcc-cross/aarch64-linux-gnu/$gcc (ubuntu)
  latest_gcc_cross_version := $(shell ls -d /usr/lib/gcc-cross/$(target_arch)-$(target_os)-gnu/* 2>/dev/null | sort -V | tail -n1 | xargs -I{} basename {})
  prefix := /usr/lib/gcc-cross/$(target_arch)-$(target_os)-gnu/$(latest_gcc_cross_version)
  ifneq ($(wildcard $(prefix)),)
    export RUSTFLAGS := $(RUSTFLAGS) -C link-args=-L$(prefix)
  endif

  # NOTE: there is also https://musl.cc/aarch64-linux-musl-cross.tgz

  $(info RUSTFLAGS = $(RUSTFLAGS))
endif

.PHONY: build build_completion deploy-binary chdig install run \
	deb rpm archlinux tar packages

# This should be the first target (since ".DEFAULT_GOAL" is supported only since 3.80+)
default: build
.DEFAULT_GOAL: default

chdig:
	cargo build $(cargo_build_opts)

run: chdig
	cargo run $(cargo_build_opts)

build: chdig deploy-binary

test:
	cargo test $(cargo_build_opts)

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
	tar --show-transformed-names --transform "s#^$${tmp_dir#/}#chdig-${CHDIG_VERSION}-${target_arch}#" -vczf chdig-${CHDIG_VERSION}-${target_arch}.tar.gz $$tmp_dir
	echo rm -fr $$tmp_dir

help:
	@echo "Usage: make [debug=1] [target=<TRIPLE>]"
