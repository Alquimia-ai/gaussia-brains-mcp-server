"""OAuth scopes understood by the service.

Advertised in the protected resource metadata (RFC 9728) and in
``insufficient_scope`` challenges (RFC 6750 §3.1). Access is deliberately
coarse: a token that may read brains may read any brain that passes the
signature policy, and every module of it.
"""

from __future__ import annotations

#: Connect to an MCP-served brain and use its tools (search, read, resolve).
READ = "brains:read"
#: List brains in the namespace indexes over REST.
CATALOG = "brains:catalog"
#: Publish, unpublish, and reindex brains in the namespace index over REST.
PUBLISH = "brains:publish"
#: Change brains through the working store: create, register evidence,
#: commit, branch, push, pull, converge and import. Writes are attributed to
#: the token's human subject. Reading a working-store brain (status, browse,
#: history, search) needs ``brains:catalog`` or ``brains:read`` instead.
WRITE = "brains:write"

ALL = (READ, CATALOG, PUBLISH, WRITE)
