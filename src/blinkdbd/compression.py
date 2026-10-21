from __future__ import annotations

import zstandard

RAW_MARKER_V2 = b"BLK2R:"
ZSTD_MARKER_V2 = b"BLK2Z:"
DEFAULT_THRESHOLD = 256


def encode(value: bytes, *, enabled: bool = True, threshold: int = DEFAULT_THRESHOLD) -> bytes:
    if not enabled:
        return RAW_MARKER_V2 + str(len(value)).encode("ascii") + b":" + value
    if len(value) < threshold:
        return RAW_MARKER_V2 + str(len(value)).encode("ascii") + b":" + value

    compressed = zstandard.ZstdCompressor(level=3).compress(value)
    compressed_header = ZSTD_MARKER_V2 + str(len(value)).encode("ascii") + b":"
    raw_header = RAW_MARKER_V2 + str(len(value)).encode("ascii") + b":"
    if len(compressed) < len(value):
        return compressed_header + compressed
    return raw_header + value


def decode(value: bytes, *, max_output_size: int) -> bytes:
    if value.startswith(RAW_MARKER_V2) or value.startswith(ZSTD_MARKER_V2):
        marker = RAW_MARKER_V2 if value.startswith(RAW_MARKER_V2) else ZSTD_MARKER_V2
        separator = value.find(b":", len(marker))
        if separator < 0:
            raise ValueError("Stored value has a malformed length header")
        length_text = value[len(marker) : separator]
        if not length_text or not length_text.isdigit():
            raise ValueError("Stored value has a malformed length header")
        expected_size = int(length_text)
        if expected_size > max_output_size:
            raise ValueError("Stored value exceeds the configured decompressed size limit")
        payload = value[separator + 1 :]
        if marker == RAW_MARKER_V2:
            if len(payload) != expected_size:
                raise ValueError("Stored raw value length does not match its header")
            return payload
        window_limit_kib = max(1024, (max_output_size + 1023) // 1024)
        result = zstandard.ZstdDecompressor(
            max_window_size=window_limit_kib,
        ).decompress(
            payload,
            max_output_size=max(1, expected_size),
            allow_extra_data=False,
        )
        if len(result) != expected_size:
            raise ValueError("Stored compressed value length does not match its header")
        return result
    if len(value) > max_output_size:
        raise ValueError("Stored value exceeds the configured decompressed size limit")
    return value
