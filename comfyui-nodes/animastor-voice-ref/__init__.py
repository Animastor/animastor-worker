# SPDX-License-Identifier: MIT
from .nodes import SaveAudioAnimastorReference

NODE_CLASS_MAPPINGS = {"SaveAudioAnimastorReference": SaveAudioAnimastorReference}
NODE_DISPLAY_NAME_MAPPINGS = {"SaveAudioAnimastorReference": "Save Audio (Animastor voice reference)"}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
