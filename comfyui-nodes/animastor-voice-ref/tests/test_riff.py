# Run: python3 -I tests/test_riff.py  (no ComfyUI needed — folder_paths is stubbed)
import json, os, struct, sys, tempfile, types, wave

HERE = os.path.dirname(os.path.abspath(__file__))
sys.modules["folder_paths"] = types.SimpleNamespace(models_dir=tempfile.gettempdir())
sys.path.insert(0, os.path.dirname(HERE))
import importlib.util
spec = importlib.util.spec_from_file_location("vref_nodes", os.path.join(os.path.dirname(HERE), "nodes.py"))
nodes = importlib.util.module_from_spec(spec); spec.loader.exec_module(nodes)


def chunks(buf):
    out, pos = [], 12
    while pos + 8 <= len(buf):
        cid, size = buf[pos:pos + 4], struct.unpack("<I", buf[pos + 4:pos + 8])[0]
        out.append((cid, buf[pos + 8:pos + 8 + size])); pos += 8 + size + (size & 1)
    return out


def test_append_info_chunk():
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "a.wav")
        with wave.open(p, "wb") as w:
            w.setnchannels(1); w.setsampwidth(2); w.setframerate(24000); w.writeframes(b"\x01\x00" * 2401)
        nodes._append_info_chunk(p, json.dumps({"asr_transcript": "Привет, мир"}, ensure_ascii=False))
        buf = open(p, "rb").read()
        assert buf[:4] == b"RIFF" and struct.unpack("<I", buf[4:8])[0] == len(buf) - 8
        ids = [c[0] for c in chunks(buf)]
        assert ids[-1] == b"LIST" and b"data" in ids, ids
        lst = chunks(buf)[-1][1]
        assert lst[:4] == b"INFO" and lst[4:8] == b"ICMT"
        n = struct.unpack("<I", lst[8:12])[0]
        assert json.loads(lst[12:12 + n].rstrip(b"\x00").decode())["asr_transcript"] == "Привет, мир"
        with wave.open(p, "rb") as w:   # still a readable WAV
            assert w.getnframes() == 2401


def test_iso():
    assert nodes._iso("Russian") == "ru" and nodes._iso("de") == "de" and nodes._iso("auto") is None


def test_mono():
    import numpy as np
    s, sr = nodes._mono_float32({"waveform": np.ones((1, 2, 10), dtype=np.float32), "sample_rate": 48000})
    assert s.shape == (10,) and sr == 48000



def test_ffmpeg_normalize_bitexact():
    import shutil
    if not shutil.which("ffmpeg"):
        print("skip ffmpeg"); return
    import numpy as np
    t = np.arange(48000, dtype=np.float32) / 48000.0
    s = (0.3 * np.sin(2 * np.pi * 220 * t)).astype(np.float32)
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "n.wav")
        nodes._ffmpeg_normalize(s, 48000, p)
        buf = open(p, "rb").read()
        assert [c[0] for c in chunks(buf)] == [b"fmt ", b"data"], [c[0] for c in chunks(buf)]
        with wave.open(p, "rb") as w:
            assert (w.getnchannels(), w.getframerate(), w.getsampwidth()) == (1, 24000, 2)
            assert abs(w.getnframes() - 24000) <= 2

if __name__ == "__main__":
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f(); print("ok", k)
