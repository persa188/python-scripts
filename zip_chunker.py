import os
import re
import zlib
import zipfile
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

def parse_size(size_str):
    match = re.match(r'^([\d.]+)\s*(KB|MB|GB)?$', size_str.strip(), re.IGNORECASE)
    if not match:
        raise argparse.ArgumentTypeError(f"Invalid size format: '{size_str}'")

    size, unit = match.groups()
    size = float(size)
    unit = (unit or "MB").upper()

    factor = {
        "KB": 1024,
        "MB": 1024 ** 2,
        "GB": 1024 ** 3
    }[unit]

    return int(size * factor)

def estimate_zip_overhead(file_count, avg_name_len=50):
    return file_count * (30 + 46 + avg_name_len) + 22

def compress_one(args):
    file_path, root_folder = args

    with open(file_path, 'rb') as f:
        data = f.read()

    compressed = zlib.compress(data, level=6)
    compressed_size = len(compressed)
    arcname = os.path.relpath(file_path, root_folder)

    return {
        'path': str(file_path),
        'arcname': arcname,
        'size': compressed_size
    }

class ZipChunker:
    def __init__(self, folder_to_zip, output_folder, max_chunk_size_bytes):
        self.folder = Path(folder_to_zip).resolve()
        self.output_folder = Path(output_folder).resolve()
        self.max_chunk_size = max_chunk_size_bytes
        self.base_name = self.folder.name

    def __enter__(self):
        self.output_folder.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # no cleanup needed anymore

    def walk_files(self):
        for path in self.folder.rglob("*"):
            if path.is_file():
                yield path

    def compress_to_estimate_parallel(self, process_count):
        files = list(self.walk_files())
        print(f"🧠 Using {process_count} processes for compression estimation...")

        results = []
        with ProcessPoolExecutor(max_workers=process_count) as executor:
            future_to_path = {
                executor.submit(compress_one, (p, str(self.folder))): p
                for p in files
            }
            for future in as_completed(future_to_path):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"❌ Error estimating {future_to_path[future]}: {e}")

        return results

    def bin_pack_files(self, files):
        bins = []
        for file in sorted(files, key=lambda x: x['size'], reverse=True):
            placed = False
            for b in bins:
                estimated_overhead = estimate_zip_overhead(len(b['files']) + 1)
                if b['size'] + file['size'] + estimated_overhead <= self.max_chunk_size:
                    b['files'].append(file)
                    b['size'] += file['size']
                    placed = True
                    break
            if not placed:
                bins.append({'files': [file], 'size': file['size']})
        return bins

    def write_bins(self, bins):
        for i, b in enumerate(bins, 1):
            if not b['files']:
                continue

            zip_path = self.output_folder / f"{self.base_name}_part{i}.zip"
            with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
                for f in b['files']:
                    zipf.write(f['path'], arcname=f['arcname'])

            real_size = zip_path.stat().st_size
            print(f"✅ Created: {zip_path} ({real_size // 1024} KB)")

    def run(self, process_count=4):
        print(f"📁 Zipping: {self.folder}")
        print(f"📦 Output: {self.output_folder}")
        print(f"🎯 Max ZIP size: {self.max_chunk_size / (1024 * 1024):.2f} MB")

        if self.max_chunk_size < 1024:
            print("⚠️ Warning: Chunk size is very small — zip overhead may exceed your limit.")

        print("🔄 Estimating compressed sizes...")
        all_files = self.compress_to_estimate_parallel(process_count)

        print("📐 Bin packing...")
        bins = self.bin_pack_files(all_files)

        print("🗜 Writing ZIP files...")
        self.write_bins(bins)

        print(f"🎉 Done — {len(bins)} ZIP file(s) created.")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Split a folder into multiple ZIP files, each under a max size limit."
    )
    parser.add_argument("folder", help="Folder to zip recursively")
    parser.add_argument("output", help="Output directory for ZIP files")
    parser.add_argument(
        "-s", "--size", type=parse_size, default=parse_size("100MB"),
        help="Max ZIP size (e.g. 500KB, 100MB, 2GB). Default: 100MB"
    )
    parser.add_argument(
        "-p", "--processes", type=int, default=os.cpu_count(),
        help="Number of processes to use (default: CPU count)"
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    with ZipChunker(args.folder, args.output, args.size) as chunker:
        chunker.run(process_count=args.processes)
