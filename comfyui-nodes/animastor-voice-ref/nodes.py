# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Animastor
"""SaveAudioAnimastorReference — the output node of the voice-reference workflows.

Contract with the Animastor worker (worker.cjs findOutputNodes / waitResult):
the worker returns the FIRST output of the first node whose class_type starts
with ``SaveAudio`` that reports ``ui.audio[0].filename``. Hence the class name.

What the node does (normative description: animastor-docs
architecture/VOICE_REFERENCES.md):
  1. mixes the AUDIO input down to mono and normalises it with ffmpeg to
     WAV, PCM s16le, mono, 24 kHz, bit-exact (no encoder tags);
  2. transcribes the normalised audio with faster-whisper (model small or
     medium, word timestamps on, no prompt / no expected text — the ASR must
     not be biased towards the phrase it is checked against);
  3. appends the transcript + word timestamps as UTF-8 JSON in a RIFF
     ``LIST/INFO/ICMT`` chunk AFTER the data chunk.
The backend parses the chunk, compares the transcript with the template phrase
(strict, after normalisation), strips the LIST chunk and stores the canonical
WAV; sha256 is computed by the backend over the stored file.

Idea of the normalisation and the reference format follow LocalText2Voice
(MIT, https://github.com/estebanstifli/LocalText2Voice — app/tts/
voice_gallery_manager.py _normalize_reference_audio); no code is copied.
"""

import json
import os
import struct
import subprocess
import tempfile
import threading
import wave

import numpy as np

import folder_paths

NODE_VERSION = "1.0.0"
SCHEMA = "animastor.voice_ref.v1"
TARGET_RATE = 24000

# Qwen3-TTS language names → ISO 639-1 (faster-whisper wants ISO codes)
LANG_TO_ISO = {
    "russian": "ru", "english": "en", "german": "de", "french": "fr",
    "spanish": "es", "italian": "it", "portuguese": "pt", "japanese": "ja",
    "korean": "ko", "chinese": "zh",
}

_MODEL_LOCK = threading.Lock()
_MODEL_CACHE = {}


def _iso(language):
    s = (language or "").strip().lower()
    if not s or s == "auto":
        return None
    if s in LANG_TO_ISO:
        return LANG_TO_ISO[s]
    return s[:2] if len(s) >= 2 else None


def _whisper_root():
    root = os.path.join(folder_paths.models_dir, "whisper")
    os.makedirs(root, exist_ok=True)
    return root


def _load_whisper(model_size, device, compute_type):
    """faster-whisper model, cached per (size, device, compute). Preloaded by the
    installer to models/whisper/faster-whisper-<size>; otherwise downloaded
    once into the same folder (under /home on E2E — survives a restart)."""
    from faster_whisper import WhisperModel  # imported lazily: ComfyUI starts without it

    key = (model_size, device, compute_type)
    with _MODEL_LOCK:
        if key not in _MODEL_CACHE:
            root = _whisper_root()
            local = os.path.join(root, f"faster-whisper-{model_size}")
            src = local if os.path.isfile(os.path.join(local, "model.bin")) else model_size
            _MODEL_CACHE[key] = WhisperModel(src, device=device, compute_type=compute_type, download_root=root)
        return _MODEL_CACHE[key]


def _device_plan(device, compute_type):
    """[(device, compute_type), …] in the order to try. auto = CUDA fp16, then CPU int8."""
    cuda_ok = False
    try:
        import ctranslate2
        cuda_ok = ctranslate2.get_cuda_device_count() > 0
    except Exception:
        cuda_ok = False
    if device == "cpu":
        return [("cpu", "int8" if compute_type == "auto" else compute_type)]
    if device == "cuda":
        return [("cuda", "float16" if compute_type == "auto" else compute_type)]
    plan = []
    if cuda_ok:
        plan.append(("cuda", "float16" if compute_type == "auto" else compute_type))
    plan.append(("cpu", "int8"))
    return plan


def _transcribe(path, model_size, device, compute_type, language):
    errors = []
    for dev, ct in _device_plan(device, compute_type):
        try:
            model = _load_whisper(model_size, dev, ct)
            segments, info = model.transcribe(
                path,
                language=_iso(language),
                beam_size=5,
                temperature=0.0,
                word_timestamps=True,
                condition_on_previous_text=False,
                vad_filter=False,
            )
            words, texts = [], []
            for seg in segments:
                texts.append(seg.text)
                for w in (seg.words or []):
                    words.append({"w": w.word, "start": round(float(w.start), 3),
                                  "end": round(float(w.end), 3), "p": round(float(w.probability), 4)})
            return {
                "asr_transcript": "".join(texts).strip(),
                "asr_language": info.language,
                "asr_language_probability": round(float(info.language_probability), 4),
                "words": words,
                "whisper_device": dev,
                "compute_type": ct,
                "fallback_errors": errors,
            }
        except Exception as exc:  # GPU path not available → next entry of the plan
            errors.append(f"{dev}/{ct}: {type(exc).__name__}: {str(exc)[:300]}")
    raise RuntimeError("faster-whisper failed on every device: " + " | ".join(errors))


def _mono_float32(audio):
    waveform = audio["waveform"]
    sr = int(audio["sample_rate"])
    if hasattr(waveform, "detach"):
        waveform = waveform.detach().cpu().float().numpy()
    arr = np.asarray(waveform, dtype=np.float32)
    if arr.ndim == 3:      # [batch, channels, samples] — the first item only
        arr = arr[0]
    if arr.ndim == 2:      # [channels, samples]
        arr = arr.mean(axis=0)
    return np.ascontiguousarray(arr.reshape(-1), dtype=np.float32), sr


def _ffmpeg_normalize(samples, sr, out_path):
    """float32 mono @sr → WAV pcm_s16le mono 24 kHz, bit-exact (no LIST/ISFT tag)."""
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "f32le", "-ar", str(sr), "-ac", "1", "-i", "pipe:0",
        "-ac", "1", "-ar", str(TARGET_RATE), "-c:a", "pcm_s16le",
        "-map_metadata", "-1", "-fflags", "+bitexact", "-flags:a", "+bitexact",
        out_path,
    ]
    r = subprocess.run(cmd, input=samples.tobytes(), capture_output=True)
    if r.returncode != 0:
        raise RuntimeError("ffmpeg normalisation failed: " + r.stderr.decode("utf-8", "replace")[-500:])


def _append_info_chunk(path, payload):
    """Append LIST/INFO/ICMT (UTF-8 JSON, NUL-terminated) after the data chunk."""
    text = payload.encode("utf-8") + b"\x00"
    if len(text) % 2:
        text += b"\x00"
    icmt = b"ICMT" + struct.pack("<I", len(text)) + text
    body = b"INFO" + icmt
    chunk = b"LIST" + struct.pack("<I", len(body)) + body
    with open(path, "r+b") as f:
        head = f.read(12)
        if head[:4] != b"RIFF" or head[8:12] != b"WAVE":
            raise RuntimeError("normalised file is not RIFF/WAVE")
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size % 2:
            f.write(b"\x00")
            size += 1
        f.write(chunk)
        f.seek(4)
        f.write(struct.pack("<I", size + len(chunk) - 8))


class SaveAudioAnimastorReference:
    """Normalise + transcribe a voice-reference candidate; save WAV with RIFF JSON."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "audio": ("AUDIO",),
                "filename_prefix": ("STRING", {"default": "voices/ref"}),
                "language": ("STRING", {"default": "Russian"}),
                "whisper_model": (["small", "medium"], {"default": "small"}),
                "device": (["auto", "cuda", "cpu"], {"default": "auto"}),
                "compute_type": (["auto", "float16", "int8_float16", "int8"], {"default": "auto"}),
            }
        }

    RETURN_TYPES = ()
    FUNCTION = "save"
    OUTPUT_NODE = True
    CATEGORY = "Animastor"

    def save(self, audio, filename_prefix, language, whisper_model, device, compute_type):
        samples, sr = _mono_float32(audio)
        if samples.size == 0:
            raise ValueError("empty audio")
        out_dir = folder_paths.get_output_directory()
        full_dir, filename, counter, subfolder, _ = folder_paths.get_save_image_path(filename_prefix, out_dir)
        os.makedirs(full_dir, exist_ok=True)
        name = f"{filename}_{counter:05}_.wav"
        final = os.path.join(full_dir, name)

        with tempfile.TemporaryDirectory(prefix="animastor-vref-") as tmp:
            norm = os.path.join(tmp, "ref.wav")
            _ffmpeg_normalize(samples, sr, norm)
            with wave.open(norm, "rb") as w:
                frames, rate, channels = w.getnframes(), w.getframerate(), w.getnchannels()
            asr = _transcribe(norm, whisper_model, device, compute_type, language)
            meta = {
                "schema": SCHEMA,
                "node_version": NODE_VERSION,
                "duration_sec": round(frames / float(rate), 3),
                "sample_rate": rate,
                "channels": channels,
                "language": language,
                "verification_model": f"faster-whisper-{whisper_model}",
                "faster_whisper_version": _version("faster-whisper"),
                "ctranslate2_version": _version("ctranslate2"),
                **asr,
            }
            _append_info_chunk(norm, json.dumps(meta, ensure_ascii=False))
            if _same_fs(tmp, full_dir):
                os.replace(norm, final)
            else:
                _copy(norm, final)

        return {"ui": {"audio": [{"filename": name, "subfolder": subfolder, "type": "output"}]}}


def _version(dist):
    try:
        from importlib.metadata import version
        return version(dist)
    except Exception:
        return None


def _same_fs(a, b):
    try:
        return os.stat(a).st_dev == os.stat(b).st_dev
    except OSError:
        return False


def _copy(src, dst):
    import shutil
    shutil.copyfile(src, dst + ".part")
    os.replace(dst + ".part", dst)
