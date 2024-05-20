import re
#import sys
import asyncio

#from rich.syntax import Syntax
#from rich.traceback import Traceback

from rich.highlighter import RegexHighlighter
from rich.theme import Theme
from rich.text import Text

from textual.app import App, ComposeResult
#from textual.containers import Container, VerticalScroll
from textual.widgets import RichLog, Footer, Header

class LogHighlighter(RegexHighlighter):
    """Apply style to anything that looks like an loguru message."""

    base_style = "snlog."
    highlights = [r"(?P<date>\d+-\d+-\d+)",
                  r"(?P<time>\d+:\d+:\d+\.\d+)",
                  r"(?P<warning>WARN)",
                  r"(?P<error>ERROR)",
                  r"(?P<fatal>FATAL)",
                  r"(?P<critical>CRITICAL)",
                  r"(?P<source>\w+:[\w_]+:\d+)",
                  r"(?P<path>^/[\w/]+)",
                  ]

class TUI(App):
    TITLE = "SyncNet"
    # SUB_TITLE = "The most important question"
    CSS_PATH = "TUI.tcss"
    BINDINGS = [ ("q", "quit", "Quit") ]

    def __init__(self):
        super(TUI, self).__init__()
        self._cache = []
        self._initialized = False
        self._task = None
        self._theme = Theme({
                "snlog.date": "green",
                "snlog.time": "green",
                "snlog.warning": "yellow",
                "snlog.error": "red",
                "snlog.fatal": "bold red",
                "snlog.critical": "bold magenta",
                "snlog.source": "cyan",
                "snlog.path": "bold red",
              })

    def set_task(self, task) -> None:
        self._task = task
    def set_mode(self, mode) -> None:
        self.sub_title = mode

    def compose(self) -> ComposeResult:
        yield Header()
        yield RichLog(id="log-view", highlight=True, markup=True)#, theme=self._theme)
        yield Footer()

    def create_text_from(self, line) -> Text:
        text = Text(line)
        result1 = re.match(r'([0-9\-]+)\s*([0-9:\.]+)\s*\|\s*(\w+)\s*\|\s*([A-Za-z0-9_:]+)\s*-\s*(.+)$', line)
        if result1:
            text.stylize("rgb(0,150,0)", result1.start(1), result1.end(1))
            text.stylize("rgb(0,175,0)", result1.start(2), result1.end(2))
            match result1.group(3):
                case "INFO":
                    text.stylize("bold white", result1.start(3), result1.end(3))
                case "WARNING":
                    text.stylize("bold yellow", result1.start(3), result1.end(3))
                case "ERROR":
                    text.stylize("bold red", result1.start(3), result1.end(3))
                case "FATAL":
                    text.stylize("magenta on white", result1.start(3), result1.end(3))
                case "CRITICAL":
                    text.stylize("yellow on red", result1.start(3), result1.end(3))
            text.stylize("cyan", result1.start(4), result1.end(4))
            text.stylize("bold white", result1.start(5), result1.end(5))
            result2 = re.search(r'(/[\w/\-\_]+)', result1.group(5))
            if result2:
                start = result1.start(5)+result2.start(1)
                end = start + (result2.end(1) - result2.start(1))
                text.stylize("red", start, end)
        return text

    def on_mount(self) -> None:
        self._initialized = True

        log = self.query_one(RichLog).focus()
        # log.highlighter = LogHighlighter()
        for item in self._cache:
            log.write(self.create_text_from(item), True)
        self._cache = []

        log.focus()

        if self._task:
            asyncio.create_task(self._task.execute())

    def write(self, data) -> None:
        if self._initialized:
            self.query_one(RichLog).write(self.create_text_from(data.strip()))
        else:
            self._cache.append(data.strip())
