# FIXME: rewrite with build.rs

debug ?=

$(info DESTDIR = $(DESTDIR))
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

.PHONY: build tfg chdig install package

chdig:
	cargo build $(release)

tfg:
	pyinstaller contrib/tfg/tfg.py --onefile
	ln -r -f -s dist/tfg dist/chdig-tfg

build: chdig tfg

install:
	cp target/$(target)/chdig $(DESTDIR)/bin/chdig$(extension)
	cp dist/chdig-tfg $(DESTDIR)/bin/chdig-tfg

package: build
	nfpm package --config chdig-nfpm.yaml --packager deb

all: build install

help:
	@echo "Usage: make [debug=1]"
