"""Stream processing engine: event-time windows, watermarks, deterministic output."""

from .errors import OutputError, ParseError, StreamProcessingError, ValidationError, WindowError
from .events import Event, WatermarkTracker, parse_event_line
from .pipeline import CHECKPOINT_VERSION, Pipeline, Result
from .windows import Window, session, sliding, tumbling

__all__ = [
    "CHECKPOINT_VERSION",
    "Event",
    "OutputError",
    "ParseError",
    "Pipeline",
    "Result",
    "StreamProcessingError",
    "ValidationError",
    "WatermarkTracker",
    "Window",
    "WindowError",
    "parse_event_line",
    "session",
    "sliding",
    "tumbling",
]

__version__ = "0.1.0"
