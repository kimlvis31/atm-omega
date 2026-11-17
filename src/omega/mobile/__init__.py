"""Responsive Android/iOS browser preview; native push and remote transport are separate."""

from importlib.resources import files

ASSETS = {
    "/mobile": ("index.html", "text/html; charset=utf-8"),
    "/mobile/": ("index.html", "text/html; charset=utf-8"),
    "/mobile/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/mobile/style.css": ("style.css", "text/css; charset=utf-8"),
    "/mobile/pwa.js": ("pwa.js", "text/javascript; charset=utf-8"),
    "/mobile/sw.js": ("sw.js", "text/javascript; charset=utf-8"),
    "/mobile/manifest.webmanifest": ("manifest.webmanifest", "application/manifest+json"),
    "/mobile/icon.svg": ("icon.svg", "image/svg+xml"),
    "/mobile/icon-192.png": ("icon-192.png", "image/png"),
    "/mobile/icon-512.png": ("icon-512.png", "image/png"),
    "/mobile/apple-touch-icon.png": ("apple-touch-icon.png", "image/png"),
}


def load_asset(path: str) -> tuple[bytes, str]:
    """Exact allowlist prevents using the static route as an arbitrary file server."""
    if path not in ASSETS:
        raise LookupError("mobile asset is unavailable")
    name, content_type = ASSETS[path]
    return files(__package__).joinpath("web", name).read_bytes(), content_type
