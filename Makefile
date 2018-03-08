#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

#
# Tools
#
TAPE :=	./node_modules/.bin/tape

#
# Makefile.defs defines variables used as part of the build process.
#
include ./tools/mk/Makefile.defs

#
# Configuration used by Makefile.defs and Makefile.targ to generate
# "check" and "docs" targets.
#
JSON_FILES =	package.json
JS_FILES :=		$(shell find lib test -name '*.js') tools/bashstyle
JSL_FILES_NODE =	$(JS_FILES)
ESLINT_FILES =	$(JS_FILES)

.PHONY: all
all:

.PHONY: test
test:
	$(NODE) $(TAPE) test/*.test.js

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.targ
