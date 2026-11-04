"""Authentication: the service is an OAuth 2.0 resource server for JWT access tokens.

No identity-provider SDK and no SPIFFE dependency: any issuer publishing
OIDC/RFC 8414 metadata and a JWKS is configured as a :class:`TrustedIssuer`.
"""

from gaussia_brains_mcp_server.auth import scopes
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.auth.verifier import InvalidTokenError, TokenVerifier

__all__ = ["InvalidTokenError", "Principal", "TokenVerifier", "scopes"]
