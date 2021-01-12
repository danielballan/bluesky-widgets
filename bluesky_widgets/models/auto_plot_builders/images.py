from ..plot_builders import Images
from .base import AutoPlotter


class AutoImages(AutoPlotter):
    def __init__(self, *, max_runs=None):
        super().__init__()
        # Map (stream_name, field) to instance of Images
        self._field_to_builder = {}
        self._max_runs = max_runs

    @property
    def max_runs(self):
        return self._max_runs

    @max_runs.setter
    def max_runs(self, value):
        if max_runs is not None:
            for builders in self._lines_instances.values():
                for builder in builders:
                    builder.max_runs = value
        self._max_runs = value

    def handle_new_stream(self, run, stream_name):
        """
        Given a run and stream name, add or update figures with images.

        Parameters
        ----------
        run : BlueskyRun
        stream_name : String
        """
        ds = run[stream_name].to_dask()
        for field in ds:
            if 2 <= ds[field].ndim < 5:
                try:
                    images = self._field_to_builder[(stream_name, field)]
                except KeyError:
                    images = Images(field=field, needs_streams=(stream_name,))
                    self._field_to_builder[(stream_name, field)] = images
                images.add_run(run)
                self.plot_builders.append(images)
                self.figures.append(images.figure)
