"""Shared atomic public-data downloads."""

from pathlib import Path
import requests


def _open_download(url, allow_insecure_ssl_fallback):
    """Open a verified download, optionally retrying after a TLS inspection error."""
    request_options = {
        "stream": True,
        "timeout": 300,
        "headers": {"User-Agent": "ReEDS-Input-Processing/1.0"},
    }
    try:
        return requests.get(url, **request_options)
    except requests.exceptions.SSLError as error:
        if not allow_insecure_ssl_fallback:
            raise RuntimeError(
                "TLS certificate verification failed. Install the required CA "
                "certificate in this environment or set "
                "raw_data.allow_insecure_ssl_fallback: true in config.yaml."
            ) from error

        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        print("WARNING: TLS certificate verification failed.")
        print("         Retrying this public raw-data download with verify=False.")
        return requests.get(url, verify=False, **request_options)


def download_file(
    url, destination, force=False, allow_insecure_ssl_fallback=False
):
    """Download one file atomically, or reuse the existing local copy."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not force:
        print(f"Using existing raw file: {destination}")
        return destination

    temporary = destination.with_suffix(destination.suffix + ".part")
    print(f"Downloading {url}")
    print(f"       into {destination}")
    try:
        with _open_download(url, allow_insecure_ssl_fallback) as response:
            response.raise_for_status()
            with temporary.open("wb") as stream:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        stream.write(chunk)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    temporary.replace(destination)
    return destination


