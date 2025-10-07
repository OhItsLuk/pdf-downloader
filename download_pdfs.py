#!/usr/bin/env python3
"""
download_pdfs.py
Uso:
  python download_pdfs.py -i urls.txt -o pdfs -w 10
"""

import argparse
import concurrent.futures
import threading
from pathlib import Path
from urllib.parse import urlparse, unquote
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os

thread_local = threading.local()

def create_session(retries=5, backoff=0.5):
    s = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(['GET', 'HEAD'])
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": "pdf-downloader/1.0"})
    return s

def get_session():
    if getattr(thread_local, "session", None) is None:
        thread_local.session = create_session()
    return thread_local.session

def filename_from_url(url):
    p = urlparse(url)
    name = os.path.basename(p.path)
    name = unquote(name.split("?")[0])  # remove query string, decode percent-encoding
    if not name:
        # fallback: build a safe name
        name = f"file_{abs(hash(url))}.pdf"
    # sanitize a little (remove path separators)
    name = name.replace("/", "_").replace("\\", "_")
    return name

def resolve_collision(path: Path) -> Path:
    if not path.exists():
        return path
    base = path.stem
    suffix = path.suffix
    i = 1
    while True:
        candidate = path.with_name(f"{base}_{i}{suffix}")
        if not candidate.exists():
            return candidate
        i += 1

def download_one(url: str, dest_dir: str, timeout: tuple):
    session = get_session()
    name = filename_from_url(url)
    dest = Path(dest_dir) / name
    dest = resolve_collision(dest)
    temp = dest.with_name(dest.name + ".part")
    try:
        with session.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(temp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        # rename temp -> final
        temp.replace(dest)
        return (url, True, str(dest))
    except Exception as e:
        # cleanup temp file
        try:
            if temp.exists():
                temp.unlink()
        except:
            pass
        return (url, False, str(e))

def main():
    parser = argparse.ArgumentParser(description="Download PDFs from a list of URLs.")
    parser.add_argument('-i', '--input', default='urls.txt', help='Arquivo com URLs (uma por linha).')
    parser.add_argument('-o', '--output', default='pdfs', help='Pasta de saída.')
    parser.add_argument('-w', '--workers', type=int, default=10, help='Número de threads paralelas.')
    parser.add_argument('-to', '--timeout', type=int, default=120, help='Timeout por download (segundos).')
    args = parser.parse_args()

    urls_file = Path(args.input)
    if not urls_file.exists():
        print(f"Arquivo de URLs não encontrado: {urls_file}")
        return

    dest_dir = Path(args.output)
    dest_dir.mkdir(parents=True, exist_ok=True)

    with open(urls_file, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]

    if not urls:
        print("Nenhuma URL encontrada no arquivo.")
        return

    workers = max(1, min(args.workers, len(urls)))
    timeout = (5, args.timeout)  # connect timeout, read timeout

    print(f"Iniciando download de {len(urls)} arquivos com {workers} workers...")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(download_one, url, str(dest_dir), timeout) for url in urls]
        for fut in concurrent.futures.as_completed(futures):
            url, ok, info = fut.result()
            if ok:
                print(f"✅ Baixado: {url} -> {info}")
            else:
                print(f"❌ Falhou: {url} -> {info}")
                results.append((url, info))

    print("Finalizado.")
    if results:
        print(f"\n{len(results)} downloads falharam. Você pode tentar novamente apenas os que falharam.")
        for u, e in results:
            print(u, "->", e)

if __name__ == "__main__":
    main()
