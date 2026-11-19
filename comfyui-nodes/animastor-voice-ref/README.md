# animastor-voice-ref (ComfyUI custom node)

`SaveAudioAnimastorReference` — the output node of the Animastor voice-reference
workflows (`tts-qwen-voice-reference`, `tts-qwen-voice-transcribe`).

1. AUDIO → mono → ffmpeg → WAV PCM s16le, mono, 24 kHz, bit-exact.
2. faster-whisper (`small` | `medium`, word timestamps, no prompt) on the
   normalised audio; `device` = `auto` (CUDA fp16, then CPU int8) | `cuda` | `cpu`.
3. Transcript, word timestamps, duration and model/device info as UTF-8 JSON in a
   RIFF `LIST/INFO/ICMT` chunk after `data`.

The class name starts with `SaveAudio` on purpose: the Animastor worker returns
the first `SaveAudio*` output of the workflow (see `worker.cjs` `findOutputNodes`).
The backend strips the LIST chunk before storing the reference.

Normative description: animastor-docs `architecture/VOICE_REFERENCES.md`.

Models: `ComfyUI/models/whisper/faster-whisper-<size>/` (preloaded by the
installer; otherwise downloaded once into the same folder).

Requires the `ffmpeg` binary on PATH.

Licence: MIT. The normalisation idea follows LocalText2Voice (MIT,
https://github.com/estebanstifli/LocalText2Voice); no code is copied.
