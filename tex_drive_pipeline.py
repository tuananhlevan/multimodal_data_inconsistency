import argparse
import datetime as dt
import io
import json
import logging
import os
import random
import re
import shutil
import sys
import subprocess
import tarfile
import time
import zipfile
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple
from dotenv import load_dotenv, set_key

load_dotenv() 

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# ---------------------------------------------------------------------------
# Logging — writes to terminal.
# ---------------------------------------------------------------------------

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("tex_pipeline")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Console handler (stdout so it can be piped / read in terminal)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger

log = _setup_logger()


# ---------------------------------------------------------------------------
# Checkpoint — persists completed Drive file IDs so runs can be resumed.
# ---------------------------------------------------------------------------
CHECKPOINT_FILE = os.path.join("checkpoint", "tex_pipeline_checkpoint.json")


import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class Checkpoint:
    """Thread-safe checkpoint backed by a JSON file.

    Schema::

        {
          "processed": {
            "<file_id>": {
              "name": "paper.tar.gz",
              "processed_at": "2026-06-18T09:00:00Z",
              "output_paths": ["structured_tex_output/paper.json"]
            },
            ...
          },
          "skipped": ["<file_id>", ...]
        }
    """

    def __init__(self, path: str = CHECKPOINT_FILE) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._data: dict = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                log.warning("Could not load checkpoint %s: %s — starting fresh.", self.path, exc)
        return {"processed": {}, "skipped": []}

    def _save(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)
        os.replace(tmp, self.path)

    def is_done(self, file_id: str) -> bool:
        with self._lock:
            return file_id in self._data["processed"] or file_id in self._data["skipped"]

    def mark_done(self, file_id: str, name: str, output_paths: List[str]) -> None:
        with self._lock:
            self._data["processed"][file_id] = {
                "name": name,
                "processed_at": dt.datetime.utcnow().isoformat() + "Z",
                "output_paths": output_paths,
            }
            self._save()
        log.debug("Checkpoint saved for file_id=%s (%s)", file_id, name)

    def mark_skipped(self, file_id: str, name: str) -> None:
        with self._lock:
            if file_id not in self._data["skipped"]:
                self._data["skipped"].append(file_id)
                self._save()
        log.debug("Checkpoint: marked unsupported file as skipped — %s", name)

    @property
    def n_processed(self) -> int:
        return len(self._data["processed"])

    @property
    def n_skipped(self) -> int:
        return len(self._data["skipped"])

SUPPORTED_ARCHIVE_SUFFIXES = (".zip", ".tar", ".tgz", ".tar.gz", ".tar.bz2", ".tbz", ".tbz2")
SUPPORTED_TEX_SUFFIXES = (".tex",)
SUPPORTED_REFERENCE_COMMANDS = (
    "ref",
    "autoref",
    "Autoref",
    "eqref",
    "pageref",
    "vref",
    "Vref",
    "nameref",
    "cref",
    "Cref",
)


@dataclass
class SourceItem:
    file_id: str
    name: str
    mime_type: str
    parent_path: str = ""


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_json_file(path: str, payload) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def sanitize_name(name: str) -> str:
    name = name.strip().replace(os.sep, "_")
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return name.strip("._") or "document"


def strip_tex_comments(tex: str) -> str:
    cleaned_lines = []
    for line in tex.splitlines():
        buffer = []
        escaped = False
        for char in line:
            if char == "%" and not escaped:
                break
            buffer.append(char)
            escaped = char == "\\" and not escaped
        cleaned_lines.append("".join(buffer))
    return "\n".join(cleaned_lines)


def read_text_file(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def read_balanced_braces(text: str, open_brace_index: int) -> Tuple[str, int]:
    if open_brace_index >= len(text) or text[open_brace_index] != "{":
        raise ValueError("Expected an opening brace at the provided index")

    depth = 0
    start_index = open_brace_index + 1
    for index in range(open_brace_index, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start_index:index], index + 1
    raise ValueError("Unbalanced braces in TeX source")


def find_command_arguments(text: str, command_name: str) -> List[str]:
    pattern = re.compile(rf"\\{re.escape(command_name)}(?:\[[^\]]*\])?\s*\{{", re.DOTALL)
    values = []
    search_start = 0
    while True:
        match = pattern.search(text, search_start)
        if not match:
            break
        try:
            value, end_index = read_balanced_braces(text, match.end() - 1)
        except ValueError:
            search_start = match.end()
            continue
        values.append(value.strip())
        search_start = end_index
    return values


def find_environment_blocks(text: str, environment_name: str) -> List[Tuple[int, int, str]]:
    escaped_env = re.escape(environment_name)
    pattern = re.compile(
        rf"\\(begin|end)\{{{escaped_env}\*?\}}",
        re.DOTALL
    )
    
    blocks = []
    stack = []
    
    for match in pattern.finditer(text):
        if match.group(1) == "begin":
            stack.append(match.start())
        elif match.group(1) == "end":
            if stack:
                start_idx = stack.pop()
                end_idx = match.end()
                blocks.append((start_idx, end_idx, text[start_idx:end_idx]))
    
    return blocks


def extract_caption(block_text: str) -> Optional[str]:
    captions = find_command_arguments(block_text, "caption")
    return captions[0] if captions else None


def extract_labels(block_text: str) -> List[str]:
    return find_command_arguments(block_text, "label")


def extract_includegraphics_paths(block_text: str) -> List[str]:
    pattern = re.compile(r"\\includegraphics(?:\[[^\]]*\])?\s*\{", re.DOTALL)
    paths = []
    search_start = 0
    while True:
        match = pattern.search(block_text, search_start)
        if not match:
            break
        try:
            path, end_index = read_balanced_braces(block_text, match.end() - 1)
        except ValueError:
            search_start = match.end()
            continue
        paths.append(path.strip())
        search_start = end_index
    return paths


def line_number_at(text: str, char_index: int) -> int:
    return text.count("\n", 0, max(char_index, 0)) + 1


def extract_context_window(text: str, start: int, end: int, radius: int = 240) -> str:
    left = max(0, start - radius)
    right = min(len(text), end + radius)
    snippet = text[left:right].strip()
    return re.sub(r"\s+", " ", snippet)


def extract_reference_paragraphs(text: str, label_keys: List[str]) -> List[str]:
    """
    For each label key (e.g. 'fig:acc', 'tab:results'), find every paragraph
    in the document body that contains a \\ref{}, \\autoref{}, \\cref{} etc.
    referencing that label. Returns the unique matching paragraphs as plain
    strings (with internal whitespace normalised to single spaces).

    A paragraph is any block of text separated from its neighbours by one or
    more blank lines.
    """
    if not label_keys:
        return []

    # Build a regex that matches any supported \ref-family command citing one
    # of this asset's labels.
    command_pattern = "|".join(re.escape(name) for name in SUPPORTED_REFERENCE_COMMANDS)
    escaped_keys = "|".join(re.escape(k) for k in label_keys)
    ref_pattern = re.compile(
        rf"\\(?:{command_pattern})\{{\s*(?:{escaped_keys})\s*}}",
        re.DOTALL,
    )

    # Split document into paragraphs on blank lines.
    paragraphs = re.split(r"\n{2,}", text)

    seen: set = set()
    result: List[str] = []
    for para in paragraphs:
        if ref_pattern.search(para):
            # Normalise whitespace so the paragraph is compact.
            normalised = re.sub(r"\s+", " ", para).strip()
            if normalised and normalised not in seen:
                seen.add(normalised)
                result.append(normalised)
    return result


def extract_first_nonempty_command(text: str, command_names: Sequence[str]) -> Optional[str]:
    for command_name in command_names:
        values = find_command_arguments(text, command_name)
        for value in values:
            normalized = re.sub(r"\s+", " ", value).strip()
            if normalized:
                return normalized
    return None


def extract_abstract(text: str) -> Optional[str]:
    pattern = re.compile(r"\\begin\{abstract\}(.*?)\\end\{abstract\}", re.DOTALL)
    match = pattern.search(text)
    if match:
        return re.sub(r"\s+", " ", match.group(1)).strip()
    return extract_first_nonempty_command(text, ["abstract"])


def extract_title(text: str) -> Optional[str]:
    return extract_first_nonempty_command(text, ["title"])


# Map lowercase substrings found in style-package names to human-readable venue labels.
_VENUE_PACKAGE_MAP: List[tuple] = [
    ("neurips",  "NeurIPS"),
    ("iclr",     "ICLR"),
    ("icml",     "ICML"),
    ("acl",      "ACL"),
    ("emnlp",    "EMNLP"),
    ("naacl",    "NAACL"),
    ("cvpr",     "CVPR"),
    ("iccv",     "ICCV"),
    ("eccv",     "ECCV"),
    ("sigir",    "SIGIR"),
    ("kdd",      "KDD"),
    ("wsdm",     "WSDM"),
    ("icdm",     "ICDM"),
    ("nips",     "NeurIPS"),
    ("aaai",     "AAAI"),
    ("ijcai",    "IJCAI"),
    ("uai",      "UAI"),
    ("aistats",  "AISTATS"),
    ("corl",     "CoRL"),
    ("icra",     "ICRA"),
    ("iros",     "IROS"),
    ("rss",      "RSS"),
    ("coling",   "COLING"),
    ("findings", "ACL Findings"),
]


def _extract_venue_from_packages(text: str) -> Optional[str]:
    """Scan \\usepackage and \\documentclass arguments for known conference style names."""
    pkg_pattern = re.compile(
        r"\\(?:usepackage|documentclass)(?:\[[^\]]*\])?\{([^}]+)\}",
        re.DOTALL,
    )
    for match in pkg_pattern.finditer(text):
        pkg_name = match.group(1).strip().lower()
        for keyword, label in _VENUE_PACKAGE_MAP:
            if keyword in pkg_name:
                # Try to pull a 4-digit year from the package name, e.g. neurips_2025 → NeurIPS 2025
                year_match = re.search(r"(20\d{2})", pkg_name)
                year = f" {year_match.group(1)}" if year_match else ""
                return f"{label}{year}"
    return None


def extract_authors(text: str) -> Optional[str]:
    """Extract author names as a clean comma-separated string.

    Strategy:
    1. Read the raw \\author{...} block.
    2. Pull out all \\textbf{Name} entries (common in NeurIPS/ICML templates).
    3. If none found, fall back to stripping all LaTeX commands and returning
       a normalised plain-text version of the author block.
    """
    raw = extract_first_nonempty_command(text, ["author", "authors"])
    if not raw:
        return None

    # Attempt 1: collect all \textbf{...} entries — these are typically author names.
    bold_names = find_command_arguments(raw, "textbf")
    # Filter out obvious non-names (email addresses, short tokens, institution strings)
    clean_names = [
        re.sub(r"\s+", " ", n).strip()
        for n in bold_names
        if n.strip() and "@" not in n and len(n.strip()) > 2 and "$" not in n
    ]
    if clean_names:
        return ", ".join(clean_names)

    # Attempt 2: strip all LaTeX commands and return normalised plain text.
    stripped = re.sub(r"\\[a-zA-Z]+(?:\[[^\]]*\])?(?:\{[^}]*\})?", " ", raw)
    stripped = re.sub(r"[{}$\^]", " ", stripped)
    stripped = re.sub(r"\s+", " ", stripped).strip()
    return stripped if stripped else None


def extract_venue(text: str, fallback_name: str) -> Optional[str]:
    """Detect the publication venue.

    Priority:
    1. Explicit \\venue / \\journal / \\conferenceinfo commands.
    2. Known conference style packages in \\usepackage / \\documentclass.
    3. fallback_name (the Drive filename) — returned as None so callers can
       distinguish a real venue from a missing one.
    """
    explicit = extract_first_nonempty_command(
        text, ["venue", "journal", "conferenceinfo", "acmConference", "institution"]
    )
    if explicit:
        return explicit
    from_pkg = _extract_venue_from_packages(text)
    if from_pkg:
        return from_pkg
    return None  # Caller decides what fallback to use


def extract_inline_references(text: str) -> List[Dict[str, object]]:
    results = []
    command_pattern = "|".join(re.escape(name) for name in SUPPORTED_REFERENCE_COMMANDS)
    reference_pattern = re.compile(rf"\\({command_pattern})\{{", re.DOTALL)
    search_start = 0
    while True:
        match = reference_pattern.search(text, search_start)
        if not match:
            break
        command_name = match.group(1)
        try:
            reference_body, end_index = read_balanced_braces(text, match.end() - 1)
        except ValueError:
            search_start = match.end()
            continue
        results.append(
            {
                "command": command_name,
                "keys": [key.strip() for key in reference_body.split(",") if key.strip()],
                "latex_code": text[match.start():end_index],
                "line": line_number_at(text, match.start()),
                "context": extract_context_window(text, match.start(), end_index),
            }
        )
        search_start = end_index
    return results


def classify_block(block_text: str) -> str:
    lowered = block_text.lower()
    if "\\includegraphics" in lowered:
        return "figure"
    if "\\begin{table" in lowered or "\\begin{tabular" in lowered or "\\begin{longtable" in lowered:
        return "table"
    if "\\begin{tikzpicture" in lowered or "\\begin{axis" in lowered or "\\addplot" in lowered:
        return "plot"
    return "unknown"


def extract_paper_assets(tex_files: List[str]) -> List[Dict[str, object]]:
    """Two-pass, paper-level asset extraction.

    **Pass 1 — collect assets across all .tex files.**
    Every ``\\begin{figure}``, ``\\begin{table}``, etc. is extracted with its
    ``\\label``, ``\\caption``, ``\\includegraphics`` paths, and raw LaTeX.
    A ``label → asset`` index is built for the second pass.

    **Pass 2 — find reference contexts across all .tex files.**
    Every paragraph in every .tex file is scanned for ``\\ref{label}`` (and
    ``\\cref``, ``\\autoref``, etc.) citations that match one of the labels
    collected in Pass 1.  Matching paragraphs are attached as
    ``reference_context`` on the corresponding asset.

    Returns the deduplicated list of assets with contexts attached.
    """
    # ------------------------------------------------------------------
    # Pass 1: collect all asset blocks from every .tex file.
    # ------------------------------------------------------------------
    # label_to_asset maps each \label{key} to the asset dict that owns it.
    # One asset may have multiple labels; each gets its own entry.
    label_to_asset: Dict[str, Dict[str, object]] = {}
    all_assets: List[Dict[str, object]] = []
    seen_spans: set = set()  # (tex_path, start, end) dedup

    for tex_path in tex_files:
        try:
            raw_tex = strip_tex_comments(read_text_file(tex_path))
        except Exception as exc:
            log.warning("Could not read %s: %s", tex_path, exc)
            continue

        for start, end, block_text in _find_all_asset_spans(raw_tex):
            span_key = (tex_path, start, end)
            if span_key in seen_spans:
                continue
            seen_spans.add(span_key)

            kind = classify_block(block_text)
            labels = extract_labels(block_text)
            asset: Dict[str, object] = {
                "kind": kind,
                "source_tex": os.path.basename(tex_path),
                "line_start": line_number_at(raw_tex, start),
                "line_end": line_number_at(raw_tex, end),
                "latex_code": block_text,
                "caption": extract_caption(block_text),
                "labels": labels,
                "includegraphics_paths": extract_includegraphics_paths(block_text),
                "reference_context": [],  # filled in Pass 2
            }
            all_assets.append(asset)
            for lbl in labels:
                label_to_asset[lbl] = asset

    if not label_to_asset and not all_assets:
        return []

    # ------------------------------------------------------------------
    # Pass 2: search every .tex file for \ref{label} paragraphs.
    # ------------------------------------------------------------------
    all_labels = list(label_to_asset.keys())
    if all_labels:
        command_pattern = "|".join(re.escape(name) for name in SUPPORTED_REFERENCE_COMMANDS)
        escaped_labels = "|".join(re.escape(k) for k in all_labels)
        ref_pattern = re.compile(
            rf"\\(?:{command_pattern})\{{\s*(?:{escaped_labels})\s*}}",
            re.DOTALL,
        )
        # Per-asset set of already-seen normalised paragraph strings (dedup).
        seen_contexts: Dict[int, set] = {id(a): set() for a in all_assets}

        for tex_path in tex_files:
            try:
                raw_tex = strip_tex_comments(read_text_file(tex_path))
            except Exception:
                continue

            paragraphs = re.split(r"\n{2,}", raw_tex)
            for para in paragraphs:
                # Quick pre-check before running the full regex.
                if "\\" not in para:
                    continue
                # Find every label cited in this paragraph.
                cited_labels: set = set()
                for m in re.finditer(
                    rf"\\(?:{command_pattern})\{{\s*([^}}]+?)\s*}}",
                    para,
                    re.DOTALL,
                ):
                    for key in m.group(1).split(","):
                        key = key.strip()
                        if key in label_to_asset:
                            cited_labels.add(key)

                if not cited_labels:
                    continue

                normalised = re.sub(r"\s+", " ", para).strip()
                if not normalised:
                    continue

                for lbl in cited_labels:
                    asset = label_to_asset[lbl]
                    asset_id = id(asset)
                    if normalised not in seen_contexts[asset_id]:
                        seen_contexts[asset_id].add(normalised)
                        asset["reference_context"].append(normalised)  # type: ignore[index]

    # ------------------------------------------------------------------
    # Filter: keep tables always; keep figures/plots only when referenced.
    # ------------------------------------------------------------------
    filtered: List[Dict[str, object]] = []
    for asset in all_assets:
        if asset["kind"] == "table" or asset["reference_context"]:
            filtered.append(asset)
    return filtered


def _find_all_asset_spans(text: str) -> List[Tuple[int, int, str]]:
    """Return all (start, end, block_text) tuples for known asset environments,
    deduplicated and sorted by position.  Nested environments (e.g. a
    ``tabular`` inside a ``table``) are included as separate entries so that
    both the outer and inner blocks are captured.
    """
    environment_order = ["figure", "table", "longtable", "tabular", "tikzpicture", "axis"]
    candidates: List[Tuple[int, int, str]] = []
    for env in environment_order:
        candidates.extend(find_environment_blocks(text, env))
    candidates.sort(key=lambda t: (t[0], t[1]))
    seen: set = set()
    deduped: List[Tuple[int, int, str]] = []
    for start, end, block_text in candidates:
        if (start, end) not in seen:
            seen.add((start, end))
            deduped.append((start, end, block_text))
    return deduped


# ---------------------------------------------------------------------------
# Per-file meta extraction (title / abstract / venue / authors).
# This is kept as a separate function so build_document_record remains simple
# and extract_paper_assets handles all cross-file asset logic.
# ---------------------------------------------------------------------------

def build_document_record(
    tex_path: str,
    source_name: str,
) -> Dict[str, object]:
    """Extract bibliographic metadata from a single .tex file.

    Assets are no longer extracted here — that is done paper-level by
    ``extract_paper_assets`` so that cross-file \\ref citations are captured.
    """
    raw_tex = read_text_file(tex_path)
    tex = strip_tex_comments(raw_tex)
    extracted_title = extract_title(tex)
    return {
        "tex_file": os.path.relpath(tex_path),
        "meta": {
            "title": extracted_title or source_name,
            "abstract": extract_abstract(tex),
            "venue": extract_venue(tex, source_name),
            "authors": extract_authors(tex),
            "source_name": source_name,
        },
    }


def find_tex_files(root_dir: str) -> List[str]:
    tex_files = []
    for current_root, _, files in os.walk(root_dir):
        for file_name in files:
            if file_name.lower().endswith(SUPPORTED_TEX_SUFFIXES):
                tex_files.append(os.path.join(current_root, file_name))
    return sorted(tex_files)


def is_archive(name: str) -> bool:
    lowered = name.lower()
    return lowered.endswith(SUPPORTED_ARCHIVE_SUFFIXES)


def is_tex_file(name: str) -> bool:
    return name.lower().endswith(SUPPORTED_TEX_SUFFIXES)


def safe_extract_zip(archive_path: str, destination_dir: str) -> None:
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            member_path = os.path.abspath(os.path.join(destination_dir, member.filename))
            if not member_path.startswith(os.path.abspath(destination_dir) + os.sep):
                raise ValueError(f"Blocked unsafe zip member path: {member.filename}")
        archive.extractall(destination_dir)


def safe_extract_tar(archive_path: str, destination_dir: str) -> None:
    with tarfile.open(archive_path) as archive:
        for member in archive.getmembers():
            member_path = os.path.abspath(os.path.join(destination_dir, member.name))
            if not member_path.startswith(os.path.abspath(destination_dir) + os.sep):
                raise ValueError(f"Blocked unsafe tar member path: {member.name}")
        archive.extractall(destination_dir)


def extract_archive(archive_path: str, destination_dir: str) -> None:
    ensure_dir(destination_dir)
    if zipfile.is_zipfile(archive_path):
        safe_extract_zip(archive_path, destination_dir)
        return
    if tarfile.is_tarfile(archive_path):
        safe_extract_tar(archive_path, destination_dir)
        return
        
    # ArXiv sometimes provides a single gzipped .tex file instead of a tar archive
    # for single-file submissions. Attempt to decompress it as a raw gzip file.
    try:
        import gzip
        with gzip.open(archive_path, 'rb') as f:
            content = f.read()
        # Since it's a single file, we save it as main.tex
        dest_file = os.path.join(destination_dir, "main.tex")
        with open(dest_file, "wb") as out:
            out.write(content)
        return
    except Exception:
        pass

    raise ValueError(f"Unsupported archive format: {archive_path}")


def archive_directory_to_tar_gz(source_dir: str, output_path: str) -> None:
    """Compresses a directory into a .tar.gz archive."""
    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def get_drive_service(client_secrets_path: Optional[str] = None, token_json_path: Optional[str] = None):
    creds = None
    if token_json_path and os.path.exists(token_json_path):
        creds = Credentials.from_authorized_user_file(token_json_path)
    else:
        token_json = os.environ.get("GDRIVE_TOKEN_JSON")
        if token_json:
            creds = Credentials.from_authorized_user_info(json.loads(token_json))

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        # Save the refreshed token to the environment and .env file
        token_json_str = creds.to_json()
        os.environ['GDRIVE_TOKEN_JSON'] = token_json_str
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        set_key(env_path, 'GDRIVE_TOKEN_JSON', token_json_str)
        log.info("Refreshed token saved to .env")
    elif not creds:
        client_secrets = client_secrets_path or os.environ.get("GDRIVE_CREDENTIALS_JSON")
        if not client_secrets:
            raise ValueError("Missing Google Drive credentials. Set GDRIVE_CREDENTIALS_JSON or pass --client-secrets.")
        flow = InstalledAppFlow.from_client_secrets_file(client_secrets, SCOPES)
        creds = flow.run_local_server(port=0)

    return build("drive", "v3", credentials=creds)


def list_drive_children(service, folder_id: str) -> List[Dict[str, str]]:
    query = f"'{folder_id}' in parents and trashed = false"
    fields = "nextPageToken, files(id, name, mimeType)"
    page_token = None
    items = []
    while True:
        response = service.files().list(q=query, fields=fields, pageToken=page_token, pageSize=1000).execute()
        items.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return items


def list_drive_files_recursive(service, folder_id: str, parent_path: str = "") -> List[SourceItem]:
    items = []
    for child in list_drive_children(service, folder_id):
        if child["mimeType"] == "application/vnd.google-apps.folder":
            child_path = os.path.join(parent_path, child["name"]) if parent_path else child["name"]
            items.extend(list_drive_files_recursive(service, child["id"], child_path))
        else:
            items.append(SourceItem(child["id"], child["name"], child["mimeType"], parent_path))
    return items


def download_drive_file(service, file_id: str, destination_path: str) -> None:
    ensure_dir(os.path.dirname(destination_path))
    request = service.files().get_media(fileId=file_id)
    with io.FileIO(destination_path, "wb") as handle:
        downloader = MediaIoBaseDownload(handle, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def upload_drive_file(service, local_path: str, folder_id: str) -> None:
    metadata = {"name": os.path.basename(local_path), "parents": [folder_id]}
    media = MediaFileUpload(local_path, resumable=True)
    
    max_retries = 6
    for attempt in range(max_retries):
        try:
            service.files().create(body=metadata, media_body=media, fields="id").execute()
            break
        except Exception as e:
            if attempt + 1 < max_retries:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                log.warning("Upload failed for %s: %s. Retrying in %.1fs...", os.path.basename(local_path), e, wait_time)
                time.sleep(wait_time)
            else:
                raise e


def upload_drive_folder_recursive(service, local_dir: str, parent_folder_id: str) -> str:
    """Create a Drive folder mirroring *local_dir* under *parent_folder_id*.

    Returns the Drive folder id of the newly created top-level folder.
    """
    folder_name = os.path.basename(local_dir)
    folder_meta = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id],
    }
    
    max_retries = 6
    drive_folder = None
    for attempt in range(max_retries):
        try:
            drive_folder = service.files().create(body=folder_meta, fields="id").execute()
            break
        except Exception as e:
            if attempt + 1 < max_retries:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                log.warning("Folder creation failed for %s: %s. Retrying in %.1fs...", folder_name, e, wait_time)
                time.sleep(wait_time)
            else:
                raise e
                
    drive_folder_id = drive_folder["id"]
    log.debug("Created Drive folder '%s' (id=%s)", folder_name, drive_folder_id)

    for entry in sorted(os.listdir(local_dir)):
        entry_path = os.path.join(local_dir, entry)
        if os.path.isdir(entry_path):
            upload_drive_folder_recursive(service, entry_path, drive_folder_id)
        elif os.path.isfile(entry_path):
            upload_drive_file(service, entry_path, drive_folder_id)
            log.debug("  Uploaded file: %s", entry)

    return drive_folder_id


def _resolve_image_path(source_dir: str, tex_img_path: str) -> Optional[str]:
    """Try to find an image file referenced via \\includegraphics.

    LaTeX paths are often relative to the .tex file and may omit the extension.
    We search *source_dir* (the root of the extracted archive) using both the
    exact path and with common image extensions appended.
    """
    # Strip leading ./ or / characters
    clean = tex_img_path.strip().lstrip("./").lstrip("/")
    candidates_base = [
        os.path.join(source_dir, clean),
        # Sometimes the path already includes a subdirectory; try the basename too
        os.path.join(source_dir, os.path.basename(clean)),
    ]
    extensions = ["", ".pdf", ".png", ".jpg", ".jpeg", ".eps", ".svg", ".tikz"]
    for base in candidates_base:
        for ext in extensions:
            candidate = base + ext
            if os.path.isfile(candidate):
                return candidate
    return None


def _copy_ref_assets(
    documents: List[Dict[str, object]],
    source_dir: str,
    ref_dir: str,
) -> int:
    """Copy every referenced image file into *ref_dir*.

    Rewrites ``includegraphics_paths`` in-place on each asset to be a path
    relative to the paper output folder (i.e. ``ref/<filename>``).

    Returns the number of files successfully copied.
    """
    ensure_dir(ref_dir)
    # Track already-copied names to avoid duplicate copies.
    copied: Dict[str, str] = {}  # original resolved path → dest basename
    n_copied = 0

    for doc in documents:
        for asset in doc.get("assets", []) or []:
            new_paths: List[str] = []
            for tex_path in asset.get("includegraphics_paths", []) or []:
                resolved = _resolve_image_path(source_dir, tex_path)
                if resolved:
                    abs_resolved = os.path.abspath(resolved)
                    if abs_resolved in copied:
                        # Already copied in a previous asset — reuse the dest name.
                        dest_basename = copied[abs_resolved]
                    else:
                        dest_basename = os.path.basename(resolved)
                        dest_path = os.path.join(ref_dir, dest_basename)
                        # Handle name collisions from different subdirectories.
                        if os.path.exists(dest_path) and abs_resolved not in copied.values():
                            stem, suffix = os.path.splitext(dest_basename)
                            dest_basename = f"{stem}_{n_copied}{suffix}"
                            dest_path = os.path.join(ref_dir, dest_basename)
                        try:
                            shutil.copy2(resolved, dest_path)
                            copied[abs_resolved] = dest_basename
                            n_copied += 1
                        except Exception as exc:
                            log.warning("Could not copy %s: %s", resolved, exc)
                            new_paths.append(tex_path)  # keep original on failure
                            continue
                    # Rewrite to a path relative to the paper folder (ref/<name>)
                    new_paths.append(os.path.join("ref", dest_basename))
                else:
                    log.debug("Image not found on disk: %s", tex_path)
                    new_paths.append(tex_path)  # keep the TeX path as-is
            asset["includegraphics_paths"] = new_paths

    return n_copied


def _copy_ref_assets_list(
    assets: List[Dict[str, object]],
    source_dir: str,
    ref_dir: str,
) -> int:
    """Flat-list version of _copy_ref_assets for use with extract_paper_assets.

    Copies every referenced image file into *ref_dir* and rewrites
    ``includegraphics_paths`` in-place to ``ref/<filename>``.

    Returns the number of files successfully copied.
    """
    ensure_dir(ref_dir)
    copied: Dict[str, str] = {}  # abs resolved path → dest basename
    n_copied = 0

    for asset in assets:
        new_paths: List[str] = []
        for tex_path in asset.get("includegraphics_paths", []) or []:
            resolved = _resolve_image_path(source_dir, tex_path)
            if resolved:
                abs_resolved = os.path.abspath(resolved)
                if abs_resolved in copied:
                    dest_basename = copied[abs_resolved]
                else:
                    dest_basename = os.path.basename(resolved)
                    dest_path = os.path.join(ref_dir, dest_basename)
                    if os.path.exists(dest_path) and abs_resolved not in copied.values():
                        stem, suffix = os.path.splitext(dest_basename)
                        dest_basename = f"{stem}_{n_copied}{suffix}"
                        dest_path = os.path.join(ref_dir, dest_basename)
                    try:
                        shutil.copy2(resolved, dest_path)
                        copied[abs_resolved] = dest_basename
                        n_copied += 1
                    except Exception as exc:
                        log.warning("Could not copy %s: %s", resolved, exc)
                        new_paths.append(tex_path)
                        continue
                new_paths.append(os.path.join("ref", dest_basename))
            else:
                log.debug("Image not found on disk: %s", tex_path)
                new_paths.append(tex_path)
        asset["includegraphics_paths"] = new_paths

    return n_copied


# ---------------------------------------------------------------------------
# Table → PDF → PNG compilation
# ---------------------------------------------------------------------------

# Common academic LaTeX packages that tables typically use.
_TABLE_STANDALONE_TEMPLATE = r"""
\documentclass[varwidth=100cm,border=4pt]{{standalone}}
\usepackage{{booktabs}}
\usepackage{{multirow}}
\usepackage{{multicol}}
\usepackage{{array}}
\usepackage{{amsmath}}
\usepackage{{amssymb}}
\usepackage{{xcolor}}
\usepackage{{colortbl}}
\usepackage{{makecell}}
\usepackage{{rotating}}
\usepackage{{adjustbox}}
\usepackage{{tabularx}}
\usepackage{{tabulary}}
\usepackage{{longtable}}
{custom_commands}
\begin{{document}}
{content}
\end{{document}}
""".strip()


def _collect_custom_commands(tex_files: List[str]) -> str:
    """Extract \\newcommand-style definitions from all .tex preambles.

    Covers \\newcommand, \\renewcommand, \\providecommand and
    \\DeclareMathOperator (with optional * and optional-arg variants).
    Definitions are returned as a single block ready for pasting into a
    LaTeX preamble.  Duplicates are suppressed.
    """
    trigger_re = re.compile(
        r'\\(newcommand|renewcommand|providecommand|DeclareMathOperator)\*?\s*\{',
        re.DOTALL,
    )
    seen: set = set()
    chunks: List[str] = []

    for path in tex_files:
        try:
            text = strip_tex_comments(read_text_file(path))
            begin_doc = text.find('\\begin{document}')
            search_text = text[:begin_doc] if begin_doc >= 0 else text

            pos = 0
            while True:
                m = trigger_re.search(search_text, pos)
                if not m:
                    break
                try:
                    _cmd_name, cursor = read_balanced_braces(search_text, m.end() - 1)
                except ValueError:
                    pos = m.end()
                    continue

                # Consume optional [n]
                opt_n = re.match(r'\s*\[(\d)\]', search_text[cursor:])
                if opt_n:
                    cursor += opt_n.end()
                    # Consume optional [default] – find the matching ]
                    opt_d = re.match(r'\s*\[', search_text[cursor:])
                    if opt_d:
                        close = search_text.find(']', cursor + opt_d.end())
                        if close >= 0:
                            cursor = close + 1

                # Consume {body}
                body_m = re.match(r'\s*\{', search_text[cursor:])
                if body_m:
                    try:
                        _body, cursor = read_balanced_braces(
                            search_text, cursor + body_m.start()
                        )
                    except ValueError:
                        pos = m.end()
                        continue

                decl = search_text[m.start():cursor].strip()
                if decl and decl not in seen:
                    seen.add(decl)
                    chunks.append(decl)
                pos = cursor
        except Exception:
            pass

    return '\n'.join(chunks)


def _remove_cite_commands(tex: str) -> str:
    r"""Remove valid ``\cite`` commands without altering malformed TeX.

    Supports starred citations and up to two optional note arguments.
    Brace-aware parsing avoids consuming text following a citation.
    """
    pattern = re.compile(r"\\cite\*?(?![A-Za-z@])")
    chunks: List[str] = []
    copied_until = 0
    search_from = 0

    while True:
        match = pattern.search(tex, search_from)
        if not match:
            break
        cursor = match.end()

        for _ in range(2):
            while cursor < len(tex) and tex[cursor].isspace():
                cursor += 1
            if cursor >= len(tex) or tex[cursor] != "[":
                break
            depth = 0
            escaped = False
            for index in range(cursor, len(tex)):
                char = tex[index]
                if char == "[" and not escaped:
                    depth += 1
                elif char == "]" and not escaped:
                    depth -= 1
                    if depth == 0:
                        cursor = index + 1
                        break
                escaped = char == "\\" and not escaped
            else:
                cursor = -1
                break

        if cursor >= 0:
            while cursor < len(tex) and tex[cursor].isspace():
                cursor += 1
        if cursor < 0 or cursor >= len(tex) or tex[cursor] != "{":
            search_from = match.end()
            continue

        try:
            _keys, end = read_balanced_braces(tex, cursor)
        except ValueError:
            search_from = match.end()
            continue

        chunks.append(tex[copied_until:match.start()])
        copied_until = end
        search_from = end

    if not chunks:
        return tex
    chunks.append(tex[copied_until:])
    return "".join(chunks)


def _extract_tabular_block(table_block: str) -> Optional[str]:
    """Extract the innermost tabular-like environment from a table block."""
    for env in ("longtable", "tabularx", "tabulary", "tabular", "array"):
        blocks = find_environment_blocks(table_block, env)
        if blocks:
            return blocks[0][2]
    return None


def _compile_table_pdf(
    table_block: str,
    dest_stem: str,
    ref_dir: str,
    source_dir: str,
    timeout: int = 60,
    custom_cmds: str = "",
) -> Optional[str]:
    """Compile a LaTeX table block to PDF, saving it in *ref_dir*.

    Runs pdflatex **twice** (required for longtable column-width calculation).
    Uses ``nonstopmode`` without ``-halt-on-error`` so tables with a few
    unresolvable custom commands still produce a PDF (with ``?`` placeholders).

    Returns ``ref/<dest_stem>.pdf`` on success, ``None`` on failure.
    """
    import tempfile
    pdflatex = shutil.which("pdflatex")
    if not pdflatex:
        return None

    inner = _extract_tabular_block(table_block)
    if not inner:
        log.debug("No tabular block found for %s — skipping compilation.", dest_stem)
        return None

    standalone_src = _TABLE_STANDALONE_TEMPLATE.format(
        content=inner,
        custom_commands=custom_cmds,
    )
    standalone_src = _remove_cite_commands(standalone_src)

    try:
        with tempfile.TemporaryDirectory() as tmp:
            tex_file = os.path.join(tmp, "table.tex")
            pdf_src = os.path.join(tmp, "table.pdf")
            with open(tex_file, "w", encoding="utf-8") as fh:
                fh.write(standalone_src)

            env = os.environ.copy()
            env["TEXINPUTS"] = f"{source_dir}:{source_dir}//:" + env.get("TEXINPUTS", "")
            cmd = [pdflatex, "-interaction=nonstopmode", "table.tex"]

            # Two passes: the second is needed for longtable to size all rows.
            for _pass in range(2):
                result = subprocess.run(
                    cmd, cwd=tmp, env=env,
                    capture_output=True, timeout=timeout,
                )

            if not os.path.isfile(pdf_src):
                log.debug(
                    "pdflatex produced no PDF for %s (rc=%d):\n%s",
                    dest_stem, result.returncode,
                    result.stdout.decode(errors="replace")[-600:],
                )
                return None

            dest_path = os.path.join(ref_dir, dest_stem + ".pdf")
            shutil.copy2(pdf_src, dest_path)
            log.debug("Compiled table PDF → %s", dest_path)
            return os.path.join("ref", dest_stem + ".pdf")
    except subprocess.TimeoutExpired:
        log.debug("pdflatex timed out for %s.", dest_stem)
    except Exception as exc:
        log.debug("pdflatex error for %s: %s", dest_stem, exc)
    return None


def _convert_pdf_to_pngs(
    pdf_path: str,
    dpi: int = 100,
    timeout: int = 60,
) -> List[str]:
    """Convert a single-page PDF to PNG, preferring PyMuPDF (fitz) for robustness.

    Falls back to pdf2image and pdftocairo. The PDF is removed only after
    a valid PNG has been written.
    """
    output_path = os.path.splitext(pdf_path)[0] + ".png"

    # Attempt 1: PyMuPDF (fitz) - Fast, robust, handles large files well
    # We run fitz in a subprocess because its underlying C library (MuPDF) is not
    # perfectly thread-safe and can crash the entire Python process with a core dump
    # if it hits a fatal error on a corrupt PDF while in a ThreadPoolExecutor.
    try:
        script = f"""
import fitz
import sys
try:
    doc = fitz.open({repr(pdf_path)})
    if len(doc) == 0:
        sys.exit(1)
    page = doc.load_page(0)
    zoom = {dpi} / 72.0
    max_dim = 2000
    if page.rect.width * zoom > max_dim:
        zoom = max_dim / page.rect.width
    if page.rect.height * zoom > max_dim:
        zoom = max_dim / page.rect.height
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    pix.save({repr(output_path)})
    doc.close()
except Exception as e:
    sys.stderr.write(str(e))
    sys.exit(1)
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True, timeout=timeout
        )
        if result.returncode != 0:
            raise RuntimeError(f"PyMuPDF subprocess failed: {result.stderr.decode(errors='replace')}")
        
        if not os.path.isfile(output_path) or os.path.getsize(output_path) == 0:
            raise RuntimeError("PyMuPDF produced an empty PNG")
        
        os.remove(pdf_path)
        log.debug("Converted table PDF → %s with PyMuPDF", output_path)
        return [output_path]
    except Exception as exc:
        log.warning("PyMuPDF failed for %s: %s; trying pdf2image.", pdf_path, exc)
        if os.path.exists(output_path):
            os.remove(output_path)

    # Attempt 2: pdf2image
    try:
        from pdf2image import convert_from_path

        images = convert_from_path(
            pdf_path, dpi=dpi, fmt="png", first_page=1, last_page=1,
            single_file=True, timeout=timeout,
        )
        if len(images) != 1:
            raise RuntimeError(f"Expected one rendered page, received {len(images)}")
        images[0].save(output_path, format="PNG")
        if not os.path.isfile(output_path) or os.path.getsize(output_path) == 0:
            raise RuntimeError("pdf2image produced an empty PNG")
        os.remove(pdf_path)
        log.debug("Converted table PDF → %s with pdf2image", output_path)
        return [output_path]
    except Exception as exc:
        log.warning("pdf2image failed for %s: %s; trying pdftocairo.", pdf_path, exc)
        if os.path.exists(output_path):
            os.remove(output_path)

    # Attempt 3: pdftocairo
    pdftocairo = shutil.which("pdftocairo")
    if not pdftocairo:
        log.error("All PDF conversion paths are unavailable for %s.", pdf_path)
        return []

    try:
        result = subprocess.run(
            [pdftocairo, "-png", "-singlefile", "-r", str(dpi),
             pdf_path, os.path.splitext(pdf_path)[0]],
            capture_output=True, timeout=timeout,
        )
        if (result.returncode != 0 or not os.path.isfile(output_path)
                or os.path.getsize(output_path) == 0):
            log.error("pdftocairo also failed for %s (rc=%d): %s",
                      pdf_path, result.returncode,
                      result.stderr.decode(errors="replace")[-600:])
            return []
        os.remove(pdf_path)
        log.debug("Converted table PDF → %s with pdftocairo fallback", output_path)
        return [output_path]
    except subprocess.TimeoutExpired:
        log.error("PDF-to-PNG fallback timed out for %s.", pdf_path)
    except Exception as exc:
        log.error("PDF-to-PNG fallback failed for %s: %s", pdf_path, exc)
    return []


def _compile_table_images(
    assets: List[Dict[str, object]],
    ref_dir: str,
    source_dir: str,
) -> int:
    """Compile every table asset and rasterize the resulting PDF to PNG.

    Custom command definitions are harvested once from *source_dir*'s .tex
    preambles and injected into every standalone document so paper-specific
    macros like ``\\ours`` or ``\\MS`` resolve correctly.

    Returns the number of table assets successfully rendered to images.
    """
    # Harvest custom commands once for the whole paper.
    tex_files = find_tex_files(source_dir)
    custom_cmds = _collect_custom_commands(tex_files)
    if custom_cmds:
        log.debug("Collected %d custom command definition(s) for table compilation.",
                  custom_cmds.count('\\newcommand') + custom_cmds.count('\\renewcommand')
                  + custom_cmds.count('\\providecommand') + custom_cmds.count('\\DeclareMathOperator'))

    n_compiled = 0
    table_idx = 0
    used_stems: set = set()
    for asset in assets:
        if asset.get("kind") != "table":
            asset.setdefault("compiled_pdf", None)
            asset.setdefault("compiled_images", [])
            continue

        labels = asset.get("labels") or []
        base_stem = sanitize_name(labels[0]) if labels else f"table_{table_idx}"
        stem = base_stem
        while stem in used_stems:
            stem = f"{base_stem}_{table_idx}"
            table_idx += 1  # prevent infinite loop in edge cases
        used_stems.add(stem)
        table_idx += 1

        latex_code = asset.get("latex_code", "")
        pdf_rel = _compile_table_pdf(latex_code, stem, ref_dir, source_dir,
                                     custom_cmds=custom_cmds)
        if pdf_rel:
            pdf_path = os.path.join(os.path.dirname(ref_dir), pdf_rel)
            image_paths = _convert_pdf_to_pngs(pdf_path)
        else:
            image_paths = []

        # Retain the legacy key only when rasterization failed and the PDF remains.
        asset["compiled_pdf"] = pdf_rel if pdf_rel and not image_paths else None
        asset["compiled_images"] = [
            os.path.join("ref", os.path.basename(path)) for path in image_paths
        ]
        if image_paths:
            n_compiled += 1
    return n_compiled


def _convert_all_ref_pdfs(
    assets: List[Dict[str, object]],
    ref_dir: str,
) -> int:
    """Convert every PDF in ``ref_dir`` and rewrite asset paths in-place.

    This final sweep handles both copied PDF figures and table PDFs whose first
    conversion attempt failed. It raises if a PDF remains, preventing an
    archive with silently unprocessed files from being uploaded.
    """
    converted: Dict[str, List[str]] = {}
    pdf_names = sorted(
        name for name in os.listdir(ref_dir) if name.lower().endswith(".pdf")
    )
    for name in pdf_names:
        pdf_path = os.path.join(ref_dir, name)
        image_paths = _convert_pdf_to_pngs(pdf_path)
        if image_paths:
            old_rel = os.path.join("ref", name)
            converted[old_rel] = [
                os.path.join("ref", os.path.basename(path)) for path in image_paths
            ]

    remaining = sorted(
        name for name in os.listdir(ref_dir) if name.lower().endswith(".pdf")
    )
    if remaining:
        log.warning(
            "PDF conversion failed for some files; they will be removed: %s",
            ", ".join(remaining)
        )
        for name in remaining:
            try:
                os.remove(os.path.join(ref_dir, name))
            except Exception as e:
                log.warning("Failed to remove unprocessed PDF %s: %s", name, e)

    for asset in assets:
        rewritten_paths = []
        for path in asset.get("includegraphics_paths", []) or []:
            if path in converted:
                rewritten_paths.extend(converted[path])
            else:
                if not path.lower().endswith(".pdf"):
                    rewritten_paths.append(path)
        asset["includegraphics_paths"] = rewritten_paths

        compiled_pdf = asset.get("compiled_pdf")
        if compiled_pdf:
            if compiled_pdf in converted:
                existing = asset.get("compiled_images", []) or []
                asset["compiled_images"] = list(dict.fromkeys(
                    existing + converted[compiled_pdf]
                ))
            asset["compiled_pdf"] = None

    return len(converted)


def _propagate_meta(documents: List[Dict[str, object]]) -> None:
    """Back-fill missing meta fields across all documents from the same paper.

    A LaTeX paper is typically split into many .tex files: only the root file
    (e.g. main.tex) contains the preamble with \\documentclass, \\usepackage,
    \\title, \\author, etc.  Sub-files (intro.tex, method.tex, …) produce
    documents with null metadata.  This function finds the first non-null value
    for each key and writes it into every document that is still missing it.
    """
    META_FIELDS = ("title", "abstract", "venue", "authors")
    # Collect the best (first non-null) value for each field.
    best: Dict[str, object] = {}
    for doc in documents:
        meta = doc.get("meta") or {}
        for field in META_FIELDS:
            if field not in best and meta.get(field):
                best[field] = meta[field]
        if len(best) == len(META_FIELDS):
            break  # All fields found — no need to scan further.

    if not best:
        return  # Nothing to propagate.

    # Back-fill missing fields in every document.
    filled = 0
    for doc in documents:
        meta = doc.setdefault("meta", {})
        for field, value in best.items():
            if not meta.get(field):
                meta[field] = value
                filled += 1
    if filled:
        log.debug("Propagated %d meta field(s) across %d document(s).", filled, len(documents))


def process_local_source(source_dir: str, output_dir: str, source_name: str) -> List[str]:
    ensure_dir(output_dir)
    source_name = source_name.split('.')[0]  # Remove file extension if present
    tex_files = find_tex_files(source_dir)
    if not tex_files:
        log.warning("[%s] No .tex files found in %s — skipping.", source_name, source_dir)
        return []

    log.info("[%s] Found %d .tex file(s). Running two-pass asset extraction...", source_name, len(tex_files))

    # ------------------------------------------------------------------
    # Step 1: Extract bibliographic metadata from each .tex file.
    # ------------------------------------------------------------------
    documents = []
    for tex_path in tex_files:
        try:
            doc = build_document_record(tex_path, source_name)
            documents.append(doc)
        except Exception as exc:
            log.error("[%s]   ✗ Failed to parse meta from %s: %s", source_name, tex_path, exc, exc_info=True)

    # Back-fill title/abstract/venue/authors from the main .tex into sub-files
    # that have no preamble and therefore no metadata of their own.
    _propagate_meta(documents)

    # ------------------------------------------------------------------
    # Step 2: Paper-level two-pass asset extraction.
    # ------------------------------------------------------------------
    assets = extract_paper_assets(tex_files)
    log.info("[%s] Pass 1+2 complete: %d asset(s) with reference context.", source_name, len(assets))

    # -----------------------------------------------------------------------
    # Determine the paper folder name from the extracted title (preferred) or
    # fall back to the sanitised source_name.
    # -----------------------------------------------------------------------
    paper_title: Optional[str] = None
    for doc in documents:
        t = doc.get("meta", {}).get("title")
        if t:
            paper_title = t
            break
    raw_folder_name = paper_title or source_name
    # Truncate to 120 chars to stay safely under filesystem limits.
    paper_folder_name = sanitize_name(raw_folder_name[:120])

    # output_dir/<paper_folder>/
    paper_output_dir = os.path.join(output_dir, paper_folder_name)
    try:
        ensure_dir(paper_output_dir)
        # output_dir/<paper_folder>/ref/
        ref_dir = os.path.join(paper_output_dir, "ref")
        ensure_dir(ref_dir)

        # Copy referenced image files into ref/ and rewrite paths in-place.
        n_copied = _copy_ref_assets_list(assets, source_dir, ref_dir)
        log.info("[%s] Copied %d referenced asset file(s) into ref/.", source_name, n_copied)

        # Compile table assets and rasterize the intermediate PDFs before upload.
        n_tables = _compile_table_images(assets, ref_dir, source_dir)
        log.info("[%s] Compiled %d table image set(s) into ref/.", source_name, n_tables)

        # Convert copied PDF figures and retry any table conversion failures.
        n_pdf_assets = _convert_all_ref_pdfs(assets, ref_dir)
        log.info("[%s] Converted %d remaining PDF asset(s) before upload.",
                 source_name, n_pdf_assets)

        json_filename = f"{sanitize_name(source_name)}.json"
        output_path = os.path.join(paper_output_dir, json_filename)

        # Flatten: emit a single document record that holds all assets and the
        # best available metadata.
        best_meta = next(
            (doc["meta"] for doc in documents if doc.get("meta", {}).get("title")),
            documents[0]["meta"] if documents else {"source_name": source_name},
        )
        payload = {
            "schema_version": 2,
            "processed_at": dt.datetime.utcnow().isoformat() + "Z",
            "source": {"source_name": source_name, "source_type": "local"},
            "meta": best_meta,
            "assets": assets,
        }
        save_json_file(output_path, payload)
        log.info("[%s] ✅ Output folder → %s  (%d asset(s), %d ref file(s))",
                 source_name, paper_output_dir, len(assets), n_copied)
        # Return the paper folder so the caller can upload the whole directory.
        return [paper_output_dir]
    except Exception:
        if os.path.exists(paper_output_dir):
            shutil.rmtree(paper_output_dir)
        raise


def remove_local_contents(path: str) -> None:
    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)
        return

    if not os.path.isdir(path):
        return

    for current_root, dirnames, filenames in os.walk(path, topdown=False):
        for file_name in filenames:
            file_path = os.path.join(current_root, file_name)
            if os.path.exists(file_path):
                os.remove(file_path)
        for dir_name in dirnames:
            dir_path = os.path.join(current_root, dir_name)
            if os.path.isdir(dir_path):
                os.rmdir(dir_path)


def process_drive_item(service, item: SourceItem, work_dir: str, output_dir: str) -> List[str]:
    ensure_dir(work_dir)
    ensure_dir(output_dir)

    staged_name = sanitize_name(item.name)
    item_work_dir = os.path.join(work_dir, staged_name)
    archive_path = os.path.join(item_work_dir, item.name)
    extracted_root = os.path.join(item_work_dir, f"{staged_name}_extracted")
    ensure_dir(item_work_dir)

    try:
        if is_tex_file(item.name):
            log.info("Downloading .tex file: %s (id=%s)", item.name, item.file_id)
            download_drive_file(service, item.file_id, archive_path)
            produced_paths = process_local_source(item_work_dir, output_dir, staged_name)
        elif is_archive(item.name):
            log.info("Downloading archive: %s (id=%s)", item.name, item.file_id)
            download_drive_file(service, item.file_id, archive_path)
            log.debug("Extracting %s → %s", item.name, extracted_root)
            extract_archive(archive_path, extracted_root)
            produced_paths = process_local_source(extracted_root, output_dir, staged_name)
        else:
            log.warning("Skipping unsupported file type: %s", item.name)
            produced_paths = []

        return produced_paths
    except Exception as exc:
        log.error("❌ Error processing item '%s': %s", item.name, exc, exc_info=True)
        raise
    finally:
        for path in (archive_path, extracted_root, item_work_dir):
            try:
                if os.path.isfile(path) or os.path.islink(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
            except Exception as cleanup_exc:
                log.warning("Cleanup failed for %s: %s", path, cleanup_exc)


_thread_local = threading.local()

def get_worker_service(client_secrets_path: Optional[str], token_json_path: Optional[str]):
    if not hasattr(_thread_local, "service"):
        _thread_local.service = get_drive_service(client_secrets_path, token_json_path)
    return _thread_local.service

def process_drive_folder(
    service,
    source_folder_id: str,
    output_folder_id: str,
    work_dir: str,
    output_dir: Optional[str] = None,
    checkpoint: Optional["Checkpoint"] = None,
    client_secrets_path: Optional[str] = None,
    token_json_path: Optional[str] = None,
    max_workers: int = 4
) -> List[str]:
    ensure_dir(work_dir)
    local_output_dir = output_dir or os.path.join(work_dir, "structured_output")
    ensure_dir(local_output_dir)

    if checkpoint is None:
        checkpoint = Checkpoint()

    log.info("Checkpoint file: %s", os.path.abspath(checkpoint.path))
    log.info("Checkpoint status on start: %d processed, %d skipped.",
             checkpoint.n_processed, checkpoint.n_skipped)

    log.info("Listing files in Drive folder id=%s ...", source_folder_id)
    items = list_drive_files_recursive(service, source_folder_id)
    log.info("Found %d file(s) in Drive folder.", len(items))

    # Separate items into to-do vs already done.
    todo = [item for item in items if not checkpoint.is_done(item.file_id)]
    already_done = len(items) - len(todo)
    if already_done:
        log.info("Skipping %d already-processed file(s). %d remaining.", already_done, len(todo))

    output_paths = []
    
    def worker_task(item):
        worker_service = get_worker_service(client_secrets_path, token_json_path)
        try:
            produced_paths = process_drive_item(worker_service, item, work_dir, local_output_dir)
            if not produced_paths:
                checkpoint.mark_skipped(item.file_id, item.name)
                return []
            
            upload_ok = True
            for produced_path in produced_paths:
                if os.path.isdir(produced_path):
                    tar_path = produced_path + ".tar.gz"
                    try:
                        log.info("  Compressing folder '%s' to '%s' before upload...", os.path.basename(produced_path), os.path.basename(tar_path))
                        archive_directory_to_tar_gz(produced_path, tar_path)
                        
                        log.info("  Uploading archive '%s' → Drive folder id=%s", os.path.basename(tar_path), output_folder_id)
                        upload_drive_file(worker_service, tar_path, output_folder_id)
                        log.info("  ✅ Archive uploaded: %s", os.path.basename(tar_path))
                    except Exception as exc:
                        log.error("  ❌ Archive upload failed for %s: %s", produced_path, exc, exc_info=True)
                        upload_ok = False
                    finally:
                        if os.path.exists(tar_path):
                            os.remove(tar_path)
                else:
                    log.info("  Uploading file '%s' → Drive folder id=%s",
                             os.path.basename(produced_path), output_folder_id)
                    try:
                        upload_drive_file(worker_service, produced_path, output_folder_id)
                        log.info("  ✅ Uploaded: %s", os.path.basename(produced_path))
                    except Exception as exc:
                        log.error("  ❌ Upload failed for %s: %s", produced_path, exc, exc_info=True)
                        upload_ok = False
            
            if upload_ok:
                checkpoint.mark_done(item.file_id, item.name, produced_paths)
            
            # Always clean up local output to save disk space
            for produced_path in produced_paths:
                try:
                    if os.path.isdir(produced_path):
                        shutil.rmtree(produced_path)
                    else:
                        os.remove(produced_path)
                    log.debug("  Cleaned up local output: %s", produced_path)
                except Exception as e:
                    log.warning("  Failed to clean up local output %s: %s", produced_path, e)
            return produced_paths
        except Exception as e:
            log.error("Worker exception on item %s: %s", item.name, e)
            return []

    log.info("Starting processing with %d max workers...", max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(worker_task, item): item for item in todo}
        for idx, future in enumerate(as_completed(future_to_item), start=1):
            item = future_to_item[future]
            try:
                paths = future.result()
                output_paths.extend(paths)
                log.info("--- Completed [%d/%d] %s", idx, len(todo), item.name)
            except Exception as exc:
                log.error("Item %s generated an exception: %s", item.name, exc)

    log.info("All done. %d new output file(s) produced. Total checkpoint: %d processed, %d skipped.",
             len(output_paths), checkpoint.n_processed, checkpoint.n_skipped)
    return output_paths


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download TeX bundles from Google Drive, extract structured metadata, and upload JSON results back to Drive.")
    parser.add_argument("--source-folder-id", type=str, default=None, help="Google Drive folder id containing archives or .tex files.")
    parser.add_argument("--input-dir", type=str, default=None, help="Local folder to process instead of Google Drive.")
    parser.add_argument("--output-folder-id", type=str, default=None, help="Google Drive folder id for JSON output uploads.")
    parser.add_argument("--output-dir", type=str, default="./structured_tex_output", help="Local folder for JSON output.")
    parser.add_argument("--work-dir", type=str, default="./tex_pipeline_work", help="Temporary working directory.")
    parser.add_argument("--client-secrets", type=str, default=None, help="Path to Google OAuth client secrets JSON.")
    parser.add_argument("--token-json", type=str, default=None, help="Path to a saved Google OAuth token JSON.")
    parser.add_argument("--delete-source", action="store_true", help="Delete the local input after successful processing.")
    parser.add_argument("--checkpoint-file", type=str, default=CHECKPOINT_FILE,
                        help=f"Path to the JSON checkpoint file (default: {CHECKPOINT_FILE}).")
    parser.add_argument("--workers", type=int, default=4, help="Number of concurrent worker threads.")
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("tex_drive_pipeline starting")
    log.info("=" * 60)

    ensure_dir(args.output_dir)
    ensure_dir(args.work_dir)

    if args.input_dir:
        log.info("Mode: local directory → %s", args.input_dir)
        local_outputs = process_local_source(
            args.input_dir,
            args.output_dir,
            os.path.basename(os.path.abspath(args.input_dir)),
        )
        if args.delete_source and os.path.exists(args.input_dir):
            log.info("Deleting source directory: %s", args.input_dir)
            remove_local_contents(args.input_dir)
        log.info("Finished. %d output file(s) produced.", len(local_outputs))
        return

    if not args.source_folder_id:
        log.error("Provide either --input-dir or --source-folder-id.")
        raise ValueError("Provide either --input-dir or --source-folder-id.")
    if not args.output_folder_id:
        log.error("When using Drive input, --output-folder-id is required.")
        raise ValueError("When using Drive input, --output-folder-id is required.")

    log.info("Mode: Google Drive  source=%s  output=%s", args.source_folder_id, args.output_folder_id)
    service = get_drive_service(client_secrets_path=args.client_secrets, token_json_path=args.token_json)
    outputs = process_drive_folder(
        service=service,
        source_folder_id=args.source_folder_id,
        output_folder_id=args.output_folder_id,
        work_dir=args.work_dir,
        output_dir=args.output_dir,
        checkpoint=Checkpoint(args.checkpoint_file),
        client_secrets_path=args.client_secrets,
        token_json_path=args.token_json,
        max_workers=args.workers
    )
    log.info("Finished. %d output file(s) produced.", len(outputs))


if __name__ == "__main__":
    main()