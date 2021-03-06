##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

from __future__ import absolute_import

import sys

# The default async io event loop and twisted reactor can only be
# set once. Therefore they must be set as early as possible.

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

from twisted.internet.error import ReactorAlreadyInstalledError

try:
    import twisted.internet.asyncioreactor
    twisted.internet.asyncioreactor.install()
except (ImportError, ReactorAlreadyInstalledError):
    pass


def get_starter():
    if sys.platform[:3].lower() == "win":
        from ZServer.Zope2.Startup.starter import WindowsZopeStarter
        return WindowsZopeStarter()
    else:
        from ZServer.Zope2.Startup.starter import UnixZopeStarter
        return UnixZopeStarter()
