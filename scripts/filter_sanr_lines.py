from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from typing import Iterable


def iter_sanr_lines(lines: Iterable[str]) -> Iterable[str]:
	"""Yield only lines that start with ``#SANR`` (ignoring leading whitespace).

	Args:
		lines: Iterable of text lines (including their trailing newline characters).

	Yields:
		Lines that begin with ``#SANR`` after any leading whitespace is removed.
	"""
	for line in lines:
		if line.lstrip().startswith("#SANR"):
			yield line


def filter_file_in_place(path: str, create_backup: bool = True) -> int:
	"""Filter a file in place, keeping only ``#SANR`` lines.

	Creates a temporary file in the same directory, writes the filtered content,
	optionally creates a ``.bak`` backup of the original, and atomically replaces
	the original file.

	Args:
		path: Path to the input file to be filtered in place.
		create_backup: Whether to create a ``.bak`` backup of the original file.

	Returns:
		The number of lines kept (i.e., lines starting with ``#SANR``).
	"""
	if not os.path.isfile(path):
		raise FileNotFoundError(f"Input file not found: {path}")

	input_dir = os.path.dirname(os.path.abspath(path)) or "."
	kept_count = 0

	# Use newline="" so Python does not alter newline characters on write
	with open(path, "r", encoding="utf-8", errors="ignore", newline="") as src, tempfile.NamedTemporaryFile(
		"w", delete=False, dir=input_dir, encoding="utf-8", newline=""
	) as tmp:
		for line in iter_sanr_lines(src):
			tmp.write(line)
			kept_count += 1

	backup_path = f"{path}.bak"
	if create_backup:
		shutil.copy2(path, backup_path)

	# Atomic replace of original with filtered temp file
	os.replace(tmp.name, path)

	return kept_count


def main(argv: list[str] | None = None) -> int:
	parser = argparse.ArgumentParser(
		description=(
			"Filter a ZRXP export file, deleting every line except those starting with #SANR. "
			"By default edits the file in place and writes a .bak backup."
		)
	)
	parser.add_argument("file", help="Path to the ZRXP file to filter in place")
	parser.add_argument(
		"--no-backup",
		action="store_true",
		help="Do not create a .bak backup of the original file",
	)

	args = parser.parse_args(argv)

	try:
		kept = filter_file_in_place(args.file, create_backup=not args.no_backup)
		print(f"Kept {kept} #SANR line(s) in: {args.file}")
		return 0
	except Exception as exc:  # noqa: BLE001 - surface clear error to user
		print(f"Error: {exc}", file=sys.stderr)
		return 1


if __name__ == "__main__":
	sys.exit(main())


