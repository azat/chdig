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
  pyoxidizer_build_opts :=
  target_type := debug
  extension := -debug
else
  cargo_build_opts := --release
  pyoxidizer_build_opts := --release
  target_type = release
  extension :=
endif

ifneq ($(target),)
    cargo_build_opts += --target $(target)

    # NOTE: right now PyOxidizer does not work with Musl, due to the following error:
    #     thread 'main' panicked at 'already borrowed: BorrowMutError', /home/azat/.cargo/registry/src/github.com-1ecc6299db9ec823/pyo3-0.17.3/src/gil.rs:433:45
    #     note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
    #
    # pyoxidizer_build_opts += --target-triple $(target)
endif

.PHONY: build flameshow chdig install deb rpm archlinux packages

chdig:
	cargo build $(cargo_build_opts)

.ONESHELL:
flameshow:
	# NOTE: we can make it fully static as well
	pyoxidizer build $(pyoxidizer_build_opts) --path contrib/flameshow
	mkdir -p dist
	cp contrib/flameshow/build/x86_64-unknown-linux-gnu/release/install/flameshow dist/chdig-flameshow
	# strip: 98MiB -> 76MiB
	# gzip compression: 76MiB -> 25MiB
	strip dist/chdig-flameshow

build: chdig flameshow link

build_completion:
	cargo run $(cargo_build_opts) -- --completion bash > dist/chdig.bash-completion

install:
	install -m755 -D -t $(DESTDIR)/bin target/$(target)/$(target_type)/chdig
	install -m755 -D -t $(DESTDIR)/bin dist/chdig-flameshow
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
