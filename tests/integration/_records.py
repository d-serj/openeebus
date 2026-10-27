"""Complete stdout records and timestamp parsing; positions are byte offsets."""
import re
import json
from pathlib import Path
from datetime import datetime


def records_since(path, after=0):
    data = Path(path).read_bytes()
    start = 0
    records = []
    for line in data.splitlines(keepends=True):
        if line.endswith(b"\n") and start >= after:
            records.append(line.decode("utf-8", errors="replace"))
        start += len(line)
    return records


def record_entity(line):
    if "from entity: " not in line:
        return None
    text = line.split("from entity: ", 1)[1].strip()
    try:
        fields = json.JSONDecoder().raw_decode(text)[0]
        if isinstance(fields, dict): fields = fields[""]
        address = {key: value for field in fields for key, value in field.items()}
        return str(address["device"]) + "/" + "/".join(str(x) for x in address["entity"])
    except (ValueError, KeyError, TypeError):
        return text if re.fullmatch(r"[^{}\s]+(?:/\d+)+", text) else None


def matches(line, marker, entity=None, expected=None, peer_ski=None):
    return (marker in line and (entity is None or record_entity(line) == entity)
            and (peer_ski is None or re.search(r"(?<!\S)ski=" + re.escape(peer_ski) + r"(?![A-Za-z0-9])", line))
            and (expected is None or re.search(r"(?<![A-Za-z0-9_.])" + re.escape(expected) + r"(?![A-Za-z0-9_.])", line)))


def timestamp_seconds(line):
    old = re.search(r"\[(\d{4}/\d{2}/\d{2}) (\d{2}:\d{2}:\d{2}):(\d{4})\]", line)
    if old:
        try:
            value = datetime.strptime(old[1] + " " + old[2], "%Y/%m/%d %H:%M:%S")
        except ValueError:
            return None
        return value.toordinal() * 86400 + value.hour * 3600 + value.minute * 60 + value.second + int(old[3]) / 10000
    new = re.search(r"(?<![\d:])(\d{2}):(\d{2}):(\d{2})\.(\d{1,6})(?!\d)", line)
    if new:
        h, m, s = map(int, new.group(1, 2, 3))
        if h < 24 and m < 60 and s < 60:
            return h * 3600 + m * 60 + s + int(new[4]) / 10 ** len(new[4])
    return None
